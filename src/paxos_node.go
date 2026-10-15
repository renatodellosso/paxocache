package paxocache

import (
	"fmt"
	"log/slog"

	mapset "github.com/deckarep/golang-set/v3"
)

const PAXOS_PREPARE_ROUTE = "/cache/prepare"
const PAXOS_REQUEST_ACCEPTANCE_ROUTE = "/cache/request-acceptance"
const PAXOS_COMMIT_ROUTE = "/paxos/commit"

type PaxosClient[T any] interface {
	Prepare(Ballot)
	RequestAcceptance(Slot[T])
	Commit(Slot[T])
}

// Should be comparable by default
type Ballot struct {
	Number uint64
	OwnerId string
}

func (this Ballot) String() string {
	return fmt.Sprintf("ballot:%v:%v", this.Number, this.OwnerId)
}

func (this Ballot) LessThan(other Ballot) bool {
	if this.Number == other.Number {
		return this.OwnerId < other.OwnerId
	}
	
	return this.Number < other.Number
}

func (this Ballot) GreaterThan(other Ballot) bool {
	return this != other && !this.LessThan(other)
}
 
type Slot[T any] struct {
	// Slot index of this slot
	Number uint64
	// Highest ballot this slot was accepted with
	Ballot Ballot
	Val T
}

type PendingSlot[T any] struct {
	Proposal Slot[T]
	AcceptedBy mapset.Set[string]
}

type ChosenSlot[T any] struct {
	Number uint64
	Val T
}

// Handles Multipaxos for one log
type PaxosNode[T any] struct {
	Id string

	client PaxosClient[T]
	logger *slog.Logger

	// Not always the number of servers we're aware of!
	PoolSize uint64

	// Have we successfully obtained a quorum for its ballot and is not aware of being superseded yet?
	IsLeader bool
	// Ballot we're using to claim leadership
	LeaderBallot *Ballot
	// Who's promised our latest leader ballot
	BallotPromisedBy mapset.Set[string]
	// Acceptances we've seen while taking leadership
	PhaseOneAcceptances map[uint64]Slot[T]

	// Highest ballot we've promised
	PromisedBallot Ballot

	// Highest slot index we've seen or promised
	HighestSlot uint64

	// What we've accepted, maps slot index -> highest balloted value
	Acceptances map[uint64]Slot[T]
	// Acceptances currently pending
	PendingAcceptances map[uint64]PendingSlot[T]
	// Slots already chosen
	Chosen map[uint64]ChosenSlot[T]
}

func NewPaxosNode[T any](id string, client PaxosClient[T], logger *slog.Logger) *PaxosNode[T] {
	return &PaxosNode[T]{
		Id: id,

		client: client,
		logger: logger,

		// We only know we exist right now
		PoolSize: 1,

		BallotPromisedBy: mapset.NewSet[string](),
		PhaseOneAcceptances: make(map[uint64]Slot[T]),

		Acceptances: make(map[uint64]Slot[T]),
		PendingAcceptances: make(map[uint64]PendingSlot[T]),
		Chosen: make(map[uint64]ChosenSlot[T]),
	}
}

func (this *PaxosNode[T]) QuorumSize() uint64 {
	return this.PoolSize / 2 + 1
}

func (this *PaxosNode[T]) TryTakeLeadership() {
	nextBallot := this.PromisedBallot.Number + 1
	if this.LeaderBallot != nil && this.LeaderBallot.Number > this.PromisedBallot.Number {
		nextBallot = this.LeaderBallot.Number + 1
	}

	this.IsLeader = false
	this.LeaderBallot = &Ballot{
		Number: nextBallot,
		OwnerId: this.Id,
	}
	this.BallotPromisedBy = mapset.NewSet[string]()
	this.PhaseOneAcceptances = make(map[uint64]Slot[T])

	this.logger.Debug("TakeLeadership", "id", this.Id, "ballot", this.LeaderBallot)

	this.client.Prepare(*this.LeaderBallot)
}

func (this *PaxosNode[T]) GainLeadership() {
	this.IsLeader = true

	// Convert phase 1 acceptances into pending acceptances
	for _, val := range this.PhaseOneAcceptances {
		val.Ballot = *this.LeaderBallot
		this.PendingAcceptances[val.Number] = PendingSlot[T] {
			Proposal: val,
			AcceptedBy: mapset.NewSet[string](),
		}

		// Try to choose things already accepted
		this.client.RequestAcceptance(val)
	}
	this.PhaseOneAcceptances = make(map[uint64]Slot[T])
}

// Returns whether we lost leadership
func (this *PaxosNode[T]) promiseBallot(ballot Ballot) bool {
	this.logger.Debug("promiseBallot", "id", this.Id, "IsLeader", this.IsLeader, "ballot", ballot, "previous", this.PromisedBallot)

	if this.PromisedBallot.GreaterThan(ballot) {
		return false // Never promise a lower ballot
	}

	this.PromisedBallot = ballot
	if this.LeaderBallot != nil && this.PromisedBallot.GreaterThan(*this.LeaderBallot) {
		this.IsLeader = false
		return true
	}

	return false
}

// Returns (success, acceptances)
func (this *PaxosNode[T]) ReceivePrepare(from string, ballot Ballot) (bool, map[uint64]Slot[T]) {
	if this.PromisedBallot.GreaterThan(ballot) {
		this.logger.Debug("ReceivePrepare", "id", this.Id, "from", from, "ballot", ballot, "success", false, "acceptances", nil)
		return false, nil
	}

	this.promiseBallot(ballot)

	this.logger.Debug("ReceivePrepare", "id", this.Id, "from", from, "ballot", ballot, "success", true, "acceptances", this.Acceptances)
	return true, this.Acceptances
}

func (this *PaxosNode[T])  ReceiveRequestAcceptance(from string, slot Slot[T]) bool {
	if this.PromisedBallot.GreaterThan(slot.Ballot) {
		this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "from", from, "ballot", slot.Ballot, "slot", slot, "success", false, "reason", "Promised higher ballot")
		return false
	}

	this.Acceptances[slot.Number] = slot
	// Never accept anything below this ballot now
	this.promiseBallot(slot.Ballot)

	this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "from", from, "ballot", slot.Ballot, "slot", slot, "success", true)
	
	return true
}

// Response from sending Prepare
func (this *PaxosNode[T]) ReceivePromise(from string, ballot Ballot, success bool, acceptances []Slot[T]) {
	this.logger.Debug("ReceivePromise", "id", this.Id, "from", from, "ballot", ballot, "success", success, "acceptances", acceptances)
	
	if !success || this.PromisedBallot.GreaterThan(ballot) || this.LeaderBallot == nil || ballot != *this.LeaderBallot || this.BallotPromisedBy.Cardinality() >= int(this.QuorumSize()) {
		return
	}

	this.BallotPromisedBy.Add(from)
	
	for _, val := range acceptances {
		// If val has a higher ballot or is the first we've seen, record it
		if prev, ok := this.PhaseOneAcceptances[val.Number]; !ok || val.Ballot.GreaterThan(prev.Ballot) {
			this.PhaseOneAcceptances[val.Number] = val
		}
	}

	if this.BallotPromisedBy.Cardinality() >= int(this.QuorumSize()) {
		// We're the leader!
		this.GainLeadership()
	}
}

// Response from sending RequestAcceptance
func (this *PaxosNode[T]) ReceiveAccept(from string, ballot Ballot, success bool, slotIndex uint64) {
	this.logger.Debug("ReceiveAccept", "id", this.Id, "from", from, "ballot", ballot, "success", success, "slotIndex", slotIndex)
	
	if !success || this.promiseBallot(ballot) {
		return
	}

	pending, ok := this.PendingAcceptances[slotIndex]
	if !ok || pending.AcceptedBy.Cardinality() >= int(this.QuorumSize()) || this.PendingAcceptances[slotIndex].Proposal.Ballot != ballot {
		return
	}

	this.PendingAcceptances[slotIndex].AcceptedBy.Add(from)
	if this.PendingAcceptances[slotIndex].AcceptedBy.Cardinality() >= int(this.QuorumSize()) {
		this.client.Commit(this.PendingAcceptances[slotIndex].Proposal)
	}
}

func (this *PaxosNode[T]) ReceiveCommit(from string, slot Slot[T]) {
	this.logger.Debug("ReceiveCommit", "id", this.Id, "from", from, "slot", slot)

	delete(this.PendingAcceptances, slot.Number)
	this.Chosen[slot.Number] = ChosenSlot[T]{
		Number: slot.Number,
		Val: slot.Val,
	}
}