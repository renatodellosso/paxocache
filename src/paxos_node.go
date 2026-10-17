package paxocache

import (
	"fmt"
	"log/slog"
	"maps"
	"math"
	"sync"

	mapset "github.com/deckarep/golang-set/v3"
)

const PAXOS_PREPARE_ROUTE = "/cache/prepare"
const PAXOS_REQUEST_ACCEPTANCE_ROUTE = "/cache/request-acceptance"
const PAXOS_COMMIT_ROUTE = "/paxos/commit"

// Allow 5 slots before pool changes take effect
const POOL_DELAY = 5

type PoolChange int

const (
	PoolChangeAdd = iota
	PoolChangeRemove
)

type PoolChangeOp struct {
	Op PoolChange
	Id string
}

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
	ChosenSlot[T]
	// Highest ballot this slot was accepted with
	Ballot Ballot
}
 
type PendingSlot[T any] struct {
	Proposal Slot[T]
	AcceptedBy mapset.Set[string]
}

type ChosenSlot[T any] struct {
	Number uint64
	Val any
}

type ChosenSlotType int
const (
	ChosenSlotT = iota
	ChosenSlotPoolChange
	ChosenSlotNoOp
)

func (this *ChosenSlot[T]) GetType() ChosenSlotType {
	switch this.Val.(type) {
	case PoolChangeOp:
		return ChosenSlotPoolChange
	}

	if this.Val == nil {
		return ChosenSlotNoOp
	}

	return ChosenSlotT
}

// Handles Multipaxos for one log
type PaxosNode[T any] struct {
	Id string
	Partition string

	mutex sync.Mutex

	client PaxosClient[T]
	logger *slog.Logger

	// Maps slot number to pool
	Pools map[uint64]mapset.Set[string]

	// Have we successfully obtained a quorum for its ballot and is not aware of being superseded yet?
	IsLeader bool
	// Ballot we're using to claim leadership
	LeaderBallot *Ballot
	// Our pool for determining current leadership
	LeadershipVoters mapset.Set[string]
	// Who's promised our latest leader ballot
	BallotPromisedBy mapset.Set[string]
	// Acceptances we've seen while taking leadership
	PhaseOneAcceptances map[uint64]Slot[T]
	RecoveredSlots mapset.Set[uint64]

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

func NewPaxosNode[T any](id string, partition string, client PaxosClient[T], logger *slog.Logger, startingPool mapset.Set[string]) *PaxosNode[T] {
	node := PaxosNode[T]{
		Id: id,
		Partition: partition,

		client: client,
		logger: logger,

		HighestSlot: POOL_DELAY,

		Pools: make(map[uint64]mapset.Set[string]),

		LeadershipVoters: mapset.NewSet[string](),
		BallotPromisedBy: mapset.NewSet[string](),
		PhaseOneAcceptances: make(map[uint64]Slot[T]),
		RecoveredSlots: mapset.NewSet[uint64](),

		Acceptances: make(map[uint64]Slot[T]),
		PendingAcceptances: make(map[uint64]PendingSlot[T]),
		Chosen: make(map[uint64]ChosenSlot[T]),
	}

	// Bootstrap our starting pools
	for i := range uint64(POOL_DELAY) {
		node.Pools[i] = startingPool
		node.Chosen[i] = ChosenSlot[T]{
			Number: i,
			Val: nil,
		}
	}

	return &node
}

// Lock beforehand
func (this *PaxosNode[T]) quorumSize(slot uint64) int {
	pool := this.GetPoolForSlot(slot)
	if pool == nil {
		return math.MaxInt
	}

	return pool.Cardinality() / 2 + 1
}

// Lock beforehand
func (this *PaxosNode[T]) isInPool(slot uint64, id string) bool {
	pool := this.GetPoolForSlot(slot)
	if pool == nil {
		return false
	}

	return pool.ContainsOne(id)
}

func applyPoolChange(prev mapset.Set[string], op PoolChangeOp) mapset.Set[string] {
	changeSet := mapset.NewSet(op.Id)
	if op.Op == PoolChangeAdd {
		return prev.Union(changeSet)
	}

	return prev.Difference(changeSet)
}

// Lock beforehand. Returns nil if we can't determine the pool
func (this *PaxosNode[T]) GetPoolForSlot(slot uint64) mapset.Set[string] {
	if pool, ok := this.Pools[slot]; ok {
		fmt.Println("Cached pool")
		return pool
	}

	if slot < POOL_DELAY {
		fmt.Println("slot < POOL_DELAY")
		return nil
	}

	prevPool := this.GetPoolForSlot(slot - POOL_DELAY)
	if prevPool == nil {
		fmt.Println("prevPool is nil")
		return nil
	}
	
	prevSlot, ok := this.Chosen[slot - POOL_DELAY];
	if !ok {
		fmt.Println("prevSlot not found")
		return nil
	}

	if prevSlot.GetType() != ChosenSlotPoolChange {
		fmt.Println("No pool change. Type:", prevSlot.GetType())
		this.Pools[slot] = prevPool
		return prevPool
	}

	fmt.Println("Found pool change")
	this.Pools[slot] = applyPoolChange(prevPool, prevSlot.Val.(PoolChangeOp))
	return this.Pools[slot]
}

func (this *PaxosNode[T]) TryTakeLeadership() {
	this.mutex.Lock()

	if pool := this.GetPoolForSlot(this.HighestSlot); pool == nil {
		this.logger.Error("PaxosNode tried to take leadership, but could not determine pool!", this.Id, this.HighestSlot)
		this.mutex.Unlock()
		return
	}

	nextBallot := this.PromisedBallot.Number + 1
	if this.LeaderBallot != nil && this.LeaderBallot.Number > this.PromisedBallot.Number {
		nextBallot = this.LeaderBallot.Number + 1
	}

	this.IsLeader = false
	this.LeaderBallot = &Ballot{
		Number: nextBallot,
		OwnerId: this.Id,
	}

	this.LeadershipVoters = this.GetPoolForSlot(this.HighestSlot)
	this.BallotPromisedBy = mapset.NewSet[string]()
	this.PhaseOneAcceptances = make(map[uint64]Slot[T])
	this.RecoveredSlots = mapset.NewSet[uint64]()

	this.logger.Debug("TakeLeadership", "id", this.Id, "ballot", this.LeaderBallot)

	leaderBallot := *this.LeaderBallot
	this.mutex.Unlock() // Unlock before outgoing messages with client

	this.client.Prepare(leaderBallot)
}

// Lock before calling
func (this *PaxosNode[T]) gainLeadership() {
	this.IsLeader = true
}

// Returns whether we lost leadership. Lock beforehand
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

// Returns (success, acceptances, promised ballot)
func (this *PaxosNode[T]) ReceivePrepare(from string, ballot Ballot) (bool, map[uint64]Slot[T], Ballot) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	
	if this.PromisedBallot.GreaterThan(ballot) {
		this.logger.Debug("ReceivePrepare", "id", this.Id, "from", from, "ballot", ballot, "success", false, "acceptances", nil)
		return false, nil, this.PromisedBallot
	}

	this.promiseBallot(ballot)

	this.logger.Debug("ReceivePrepare", "id", this.Id, "from", from, "ballot", ballot, "success", true, "acceptances", this.Acceptances)
	return true, maps.Clone(this.Acceptances), this.PromisedBallot // Return a fixed snapshot of acceptances
}

func (this *PaxosNode[T]) ReceiveRequestAcceptance(from string, slot Slot[T]) (bool, Ballot) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.PromisedBallot.GreaterThan(slot.Ballot) {
		this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "from", from, "ballot", slot.Ballot, "slot", slot, "success", false, "reason", "Promised higher ballot")
		return false, this.PromisedBallot
	}

	if !this.isInPool(slot.Number, this.Id) {
		this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "from", from, "ballot", slot.Ballot, "slot", slot, "success", false, "reason", "Not in pool for that slot")
		return false, this.PromisedBallot
	}

	this.Acceptances[slot.Number] = slot
	// Never accept anything below this ballot now
	this.promiseBallot(slot.Ballot)

	this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "from", from, "ballot", slot.Ballot, "slot", slot, "success", true)
	
	return true, this.PromisedBallot
}

// Response from sending Prepare
func (this *PaxosNode[T]) ReceivePromise(from string, ballot Ballot, success bool, acceptances []Slot[T]) {
	this.mutex.Lock()

	this.logger.Debug("ReceivePromise", "id", this.Id, "from", from, "ballot", ballot, "success", success, "acceptances", acceptances)
	
	if !success || this.LeaderBallot == nil || this.PromisedBallot.GreaterThan(*this.LeaderBallot) || ballot != *this.LeaderBallot {
		this.mutex.Unlock() // Make sure to unlock!
		return
	}

	// Might be older than our current voter pool, but we still need it for recovery
	this.BallotPromisedBy.Add(from)
	
	for _, val := range acceptances {
		// If val has a higher ballot or is the first we've seen, record it
		if prev, ok := this.PhaseOneAcceptances[val.Number]; !this.RecoveredSlots.Contains(val.Number) && (!ok || val.Ballot.GreaterThan(prev.Ballot)) {
			this.PhaseOneAcceptances[val.Number] = val
		}
	}

	if this.BallotPromisedBy.Intersect(this.LeadershipVoters).Cardinality() >= this.LeadershipVoters.Cardinality() / 2 + 1 {
		// We're the leader!
		this.gainLeadership()
	}

	// Convert phase 1 acceptances into pending acceptances
	toRequest := []Slot[T]{}
	for num, val := range this.PhaseOneAcceptances {
		if pool := this.GetPoolForSlot(num); pool == nil || pool.Intersect(this.BallotPromisedBy).Cardinality() < this.quorumSize(num) {
			continue
		}

		val.Ballot = *this.LeaderBallot
		this.PendingAcceptances[num] = PendingSlot[T] {
			Proposal: val,
			AcceptedBy: mapset.NewSet[string](),
		}
		delete(this.PhaseOneAcceptances, num)
		this.RecoveredSlots.Add(num)

		toRequest = append(toRequest, val)
	}

	this.mutex.Unlock()

	// Try to choose things already accepted
	for _, slot := range toRequest {
		this.client.RequestAcceptance(slot)
	}
}

// Response from sending RequestAcceptance
func (this *PaxosNode[T]) ReceiveAccept(from string, ballot Ballot, success bool, slotIndex uint64) {
	this.mutex.Lock()
	
	this.logger.Debug("ReceiveAccept", "id", this.Id, "from", from, "ballot", ballot, "success", success, "slotIndex", slotIndex)
	
	if !success || this.promiseBallot(ballot) || !this.isInPool(slotIndex, from) {
		this.mutex.Unlock()
		return
	}

	pending, ok := this.PendingAcceptances[slotIndex]
	if !ok || pending.AcceptedBy.Cardinality() >= this.quorumSize(slotIndex) || this.PendingAcceptances[slotIndex].Proposal.Ballot != ballot {
		this.mutex.Unlock()
		return
	}

	this.PendingAcceptances[slotIndex].AcceptedBy.Add(from)
	if this.PendingAcceptances[slotIndex].AcceptedBy.Cardinality() < this.quorumSize(slotIndex) {
		this.mutex.Unlock()
		return
	}

	proposal := this.PendingAcceptances[slotIndex].Proposal
	this.mutex.Unlock()
	this.client.Commit(proposal) // No reading fields after unlocking!
}

func (this *PaxosNode[T]) ReceiveCommit(from string, slot Slot[T]) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.logger.Debug("ReceiveCommit", "id", this.Id, "from", from, "slot", slot)

	delete(this.PendingAcceptances, slot.Number)
	this.Chosen[slot.Number] = ChosenSlot[T]{
		Number: slot.Number,
		Val: slot.Val,
	}
}