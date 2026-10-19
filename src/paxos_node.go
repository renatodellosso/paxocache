package paxocache

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v3"
)

const PAXOS_PREPARE_ROUTE = "/cache/prepare"
const PAXOS_REQUEST_ACCEPTANCE_ROUTE = "/cache/request-acceptance"
const PAXOS_COMMIT_ROUTE = "/cache/commit"
const PAXOS_PULL_ROUTE = "/cache/pull"
const PAXOS_JOIN_ROUTE = "/cache/join"
const PAXOS_PROPOSE_ROUTE = "/cache/propose"

// Allow 5 slots before pool changes take effect
const POOL_DELAY = 5

const TIMEOUT = 60 * time.Second

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
	// Attempt to be added to the pool
	Join(from string) error
	Propose(val T) error
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

func (this *ChosenSlot[TOp]) GetType() ChosenSlotType {
	switch this.Val.(type) {
	case PoolChangeOp:
		return ChosenSlotPoolChange
	}

	if this.Val == nil {
		return ChosenSlotNoOp
	}

	return ChosenSlotT
}

// Data needed to join the cluster
type PaxosCoreData[TOp comparable, TRes any] struct {
	Partition string
	Leader string

	// Maps slot number to pool
	Pools map[uint64]mapset.Set[string]

	// Highest ballot we've promised
	PromisedBallot Ballot

	// Highest slot index we've seen or promised
	HighestSlot uint64

	// Slots already chosen
	Chosen map[uint64]ChosenSlot[TOp]
}

// Handles Multipaxos for one log
type PaxosNode[TOp comparable, TRes any] struct {
	PaxosCoreData[TOp, TRes]

	Id string
	mutex sync.Mutex

	client PaxosClient[TOp]
	logger *slog.Logger

	// Ballot we're using to claim leadership
	LeaderBallot *Ballot
	// Our pool for determining current leadership
	LeadershipVoters mapset.Set[string]
	// Who's promised our latest leader ballot
	BallotPromisedBy mapset.Set[string]
	// Acceptances we've seen while taking leadership
	PhaseOneAcceptances map[uint64]Slot[TOp]
	RecoveredSlots mapset.Set[uint64]

	// What we've accepted, maps slot index -> highest balloted value
	Acceptances map[uint64]Slot[TOp]
	// Acceptances currently pending
	PendingAcceptances map[uint64]PendingSlot[TOp]

	ProcessOperation func(op TOp, prev TRes) TRes
}

func NewPaxosNode[TOp comparable, TRes any](id string, partition string, client PaxosClient[TOp], logger *slog.Logger, startingPool mapset.Set[string]) *PaxosNode[TOp, TRes] {
	node := PaxosNode[TOp, TRes]{
		Id: id,
		Partition: partition,

		client: client,
		logger: logger,

		HighestSlot: POOL_DELAY,

		Pools: make(map[uint64]mapset.Set[string]),

		LeadershipVoters: mapset.NewSet[string](),
		BallotPromisedBy: mapset.NewSet[string](),
		PhaseOneAcceptances: make(map[uint64]Slot[TOp]),
		RecoveredSlots: mapset.NewSet[uint64](),

		Acceptances: make(map[uint64]Slot[TOp]),
		PendingAcceptances: make(map[uint64]PendingSlot[TOp]),
		Chosen: make(map[uint64]ChosenSlot[TOp]),
	}

	// Bootstrap our starting pools
	for i := range uint64(POOL_DELAY) {
		node.Pools[i] = startingPool
		node.Chosen[i] = ChosenSlot[TOp]{
			Number: i,
			Val: nil,
		}
	}

	return &node
}

func (this *PaxosNode[TOp, TRes]) getCoreData() PaxosCoreData[TOp, TRes] {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	
	// Copy everything in the core data struct
	data := this.PaxosCoreData
	data.Chosen = maps.Clone(data.Chosen)

	data.Pools = make(map[uint64]mapset.Set[string])
	for slot, pool := range this.Pools {
		data.Pools[slot] = pool.Clone()
	}

	return data
}

// Lock beforehand
func (this *PaxosNode[TOp, TRes]) quorumSize(slot uint64) int {
	pool := this.GetPoolForSlot(slot)
	if pool == nil {
		return math.MaxInt
	}

	return pool.Cardinality() / 2 + 1
}

// Lock beforehand
func (this *PaxosNode[TOp, TRes]) isInPool(slot uint64, id string) bool {
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
func (this *PaxosCoreData[TOp, TRes]) GetPoolForSlot(slot uint64) mapset.Set[string] {
	if pool, ok := this.Pools[slot]; ok {
		return pool
	}

	if slot < POOL_DELAY {
		return nil
	}

	prevPool := this.GetPoolForSlot(slot - POOL_DELAY)
	if prevPool == nil {
		return nil
	}
	
	prevSlot, ok := this.Chosen[slot - POOL_DELAY];
	if !ok {
		return nil
	}

	if prevSlot.GetType() != ChosenSlotPoolChange {
		this.Pools[slot] = prevPool
		return prevPool
	}

	this.Pools[slot] = applyPoolChange(prevPool, prevSlot.Val.(PoolChangeOp))
	return this.Pools[slot]
}

func (this *PaxosNode[TOp, TRes]) TryTakeLeadership() {
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

	this.LeaderBallot = &Ballot{
		Number: nextBallot,
		OwnerId: this.Id,
	}

	this.LeadershipVoters = this.GetPoolForSlot(this.HighestSlot)
	this.BallotPromisedBy = mapset.NewSet[string]()
	this.PhaseOneAcceptances = make(map[uint64]Slot[TOp])
	this.RecoveredSlots = mapset.NewSet[uint64]()

	this.logger.Debug("TakeLeadership", "id", this.Id, "ballot", this.LeaderBallot)

	leaderBallot := *this.LeaderBallot
	this.mutex.Unlock() // Unlock before outgoing messages with client

	this.client.Prepare(leaderBallot)
}

// Lock before calling
func (this *PaxosNode[TOp, TRes]) gainLeadership() {
	this.Leader = this.Id
}

func (this *PaxosNode[TOp, TRes]) IsLeader() bool {
	return this.Leader == this.Id
}

// Returns whether we lost leadership. Lock beforehand
func (this *PaxosNode[TOp, TRes]) promiseBallot(ballot Ballot) bool {
	this.logger.Debug("promiseBallot", "id", this.Id, "IsLeader", this.IsLeader(), "ballot", ballot, "previous", this.PromisedBallot)

	if this.PromisedBallot.GreaterThan(ballot) {
		return false // Never promise a lower ballot
	}

	this.PromisedBallot = ballot
	if this.LeaderBallot != nil && this.PromisedBallot.GreaterThan(*this.LeaderBallot) {
		return true
	}

	return false
}

// Returns (success, acceptances, promised ballot)
func (this *PaxosNode[TOp, TRes]) ReceivePrepare(from string, ballot Ballot) (bool, map[uint64]Slot[TOp], Ballot) {
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

func (this *PaxosNode[TOp, TRes]) ReceiveRequestAcceptance(from string, slot Slot[TOp]) (bool, Ballot) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.Leader = from

	if this.PromisedBallot.GreaterThan(slot.Ballot) {
		this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "from", from, "ballot", slot.Ballot, "slot", slot, "success", false, "reason", "Promised higher ballot")
		return false, this.PromisedBallot
	}

	if !this.isInPool(slot.Number, this.Id) {
		this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "from", from, "ballot", slot.Ballot, "slot", slot, "success", false, "reason", "Not in pool for that slot")
		return false, this.PromisedBallot
	}

	if slot.Number > this.HighestSlot {
		this.HighestSlot = slot.Number
	}

	this.Acceptances[slot.Number] = slot
	// Never accept anything below this ballot now
	this.promiseBallot(slot.Ballot)

	this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "from", from, "ballot", slot.Ballot, "slot", slot, "success", true)
	
	return true, this.PromisedBallot
}

// Response from sending Prepare
func (this *PaxosNode[TOp, TRes]) ReceivePromise(from string, ballot Ballot, success bool, acceptances []Slot[TOp]) {
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
	toRequest := []Slot[TOp]{}
	for num, val := range this.PhaseOneAcceptances {
		if pool := this.GetPoolForSlot(num); pool == nil || pool.Intersect(this.BallotPromisedBy).Cardinality() < this.quorumSize(num) {
			continue
		}

		val.Ballot = *this.LeaderBallot
		this.PendingAcceptances[num] = PendingSlot[TOp] {
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
func (this *PaxosNode[TOp, TRes]) ReceiveAccept(from string, ballot Ballot, success bool, slotIndex uint64) {
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

func (this *PaxosNode[TOp, TRes]) ReceiveCommit(from string, slot Slot[TOp]) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.logger.Debug("ReceiveCommit", "id", this.Id, "from", from, "slot", slot)

	this.Leader = from


	if slot.Number > this.HighestSlot {
		this.HighestSlot = slot.Number
	}

	delete(this.PendingAcceptances, slot.Number)
	this.Chosen[slot.Number] = ChosenSlot[TOp]{
		Number: slot.Number,
		Val: slot.Val,
	}
}

// Blocks until value is committed. Also see ProposePoolChange and ProposeNoOp
func (this *PaxosNode[TOp, TRes]) Propose(val TOp) (uint64, error) {
	return this.ProposeWithTimeout(val, TIMEOUT)
}

// Blocks until value is committed.
func (this *PaxosNode[TOp, TRes]) ProposePoolChange(op PoolChange, id string) (uint64, error) {
	return this.ProposeWithTimeout(PoolChangeOp{
		Op: op,
		Id: id,
	}, TIMEOUT)
}

func (this *PaxosNode[TOp, TRes]) ProposeNoOp() (uint64, error) {
	return this.ProposeWithTimeout(nil, TIMEOUT)
}

// Returns (selected slot, error)
func (this *PaxosNode[TOp, TRes]) ProposeWithTimeout(val any, timeout time.Duration) (uint64, error) {
	this.mutex.Lock()
	
	slotNumber := this.HighestSlot + 1

	this.logger.Debug("Proposing...", "slot", slotNumber, "val", val, "timeout", timeout)
	
	ballot := *this.LeaderBallot

	this.mutex.Unlock() // Unlock while call runs
	go this.client.RequestAcceptance(Slot[TOp]{
		Number: slotNumber,
		Ballot: ballot,
		Val: val,
	})

	// Wait for something to be committed in that slot
	wait := time.Millisecond * 50
	for timeout > 0 {
		time.Sleep(wait)

		timeout -= wait
		wait *= 2

		this.mutex.Lock()
		_, ok := this.Chosen[slotNumber]
		this.mutex.Unlock()
		if ok {
			break
		}
	}

	this.mutex.Lock()
	chosen, ok := this.Chosen[slotNumber]
	this.mutex.Unlock()

	if !ok {
		return 0, errors.New("Ran out of time")
	}

	if chosen.Val != val {
		// Something else was chosen try again if we have time left
		if timeout < 0 {
			return 0, errors.New("Something else was chosen and no time was left to retry")
		}

		return this.ProposeWithTimeout(val, timeout)
	}

	// Success!
	return slotNumber, nil
}

func (this *PaxosNode[TOp, TRes]) GetState() TRes {
	var res TRes

	expectedSlot := uint64(0)
	for slot, val := range this.Chosen {
		if slot > expectedSlot {
			break
		}
		expectedSlot++

		if val.GetType() == ChosenSlotT {
			res = this.ProcessOperation(val.Val.(TOp), res)
		}
	}

	return res
}