package paxocache

import (
	"errors"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v3"
)

// Handles Multipaxos for one log
type PaxosNode[TOp comparable, TRes any] struct {
	PaxosPersistentData[TOp, TRes]

	mutex sync.Mutex

	client    PaxosClient[TOp]
	logger    *slog.Logger
	persistor PaxosPersistor[TOp, TRes]

	// Ballot we're using to claim leadership
	LeaderBallot *Ballot
	// Our pool for determining current leadership
	LeadershipVoters mapset.Set[string]
	// Who's promised our latest leader ballot
	BallotPromisedBy mapset.Set[string]
	// Acceptances we've seen while taking leadership
	PhaseOneAcceptances map[uint64]Slot[TOp]
	RecoveredSlots      mapset.Set[uint64]

	// Acceptances currently pending
	PendingAcceptances map[uint64]PendingSlot[TOp]

	ProcessOperation func(op TOp, prev TRes) TRes
}

func NewPaxosNode[TOp comparable, TRes any](id string, partition string, client PaxosClient[TOp], logger *slog.Logger, persistor PaxosPersistor[TOp, TRes], startingPool mapset.Set[string]) *PaxosNode[TOp, TRes] {
	data := PaxosPersistentData[TOp, TRes]{
		Id: id,
		Partition:   partition,
		HighestSlot: POOL_DELAY - 1,
		Pools:       make(map[uint64]mapset.Set[string]),
		Chosen:      make(map[uint64]ChosenSlot[TOp]),
		Acceptances: make(map[uint64]Slot[TOp]),
	}

	node := NewPaxosNodeFromPersistentData(id, client, logger, persistor, &data)

	// Bootstrap our starting pools
	for i := range uint64(POOL_DELAY) {
		node.Pools[i] = startingPool

		var none TOp
		slot := ChosenSlot[TOp]{
			Number:    i,
			Operation: none,
		}
		node.Chosen[i] = slot

		persistor.AppendChosen(&slot)
	}

	return node
}

func NewPaxosNodeFromPersistentData[TOp comparable, TRes any](id string, client PaxosClient[TOp], logger *slog.Logger, persistor PaxosPersistor[TOp, TRes], data *PaxosPersistentData[TOp, TRes]) *PaxosNode[TOp, TRes] {
	node := PaxosNode[TOp, TRes]{
		PaxosPersistentData: *data,

		client:    client,
		logger:    logger,
		persistor: persistor,

		LeadershipVoters:    mapset.NewSet[string](),
		BallotPromisedBy:    mapset.NewSet[string](),
		PhaseOneAcceptances: make(map[uint64]Slot[TOp]),
		RecoveredSlots:      mapset.NewSet[uint64](),

		PendingAcceptances: make(map[uint64]PendingSlot[TOp]),
	}

	persistor.WriteSingleData(SingleData(&node.PaxosPersistentData))

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

	return pool.Cardinality()/2 + 1
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

	prevSlot, ok := this.Chosen[slot-POOL_DELAY]
	if !ok {
		return nil
	}

	if prevSlot.Type != ChosenSlotPoolChange {
		this.Pools[slot] = prevPool
		return prevPool
	}

	this.Pools[slot] = applyPoolChange(prevPool, *prevSlot.PoolChange)
	return this.Pools[slot]
}

// Blocks until leadership is gained or time runs out
func (this *PaxosNode[TOp, TRes]) TryTakeLeadership(timeout time.Duration) error {
	this.mutex.Lock()

	if pool := this.GetPoolForSlot(this.HighestSlot); pool == nil {
		err := "PaxosNode tried to take leadership, but could not determine pool!"
		this.logger.Error(err, this.Id, this.HighestSlot)
		this.mutex.Unlock()
		return errors.New(err)
	}

	nextBallot := this.PromisedBallot.Number + 1
	if this.LeaderBallot != nil && this.LeaderBallot.Number > this.PromisedBallot.Number {
		nextBallot = this.LeaderBallot.Number + 1
	}

	this.LeaderBallot = &Ballot{
		Number:  nextBallot,
		OwnerId: this.Id,
	}

	this.LeadershipVoters = this.GetPoolForSlot(this.HighestSlot)
	this.BallotPromisedBy = mapset.NewSet[string]()
	this.PhaseOneAcceptances = make(map[uint64]Slot[TOp])
	this.RecoveredSlots = mapset.NewSet[uint64]()

	this.logger.Debug("TryTakeLeadership", "id", this.Id, "partition", this.Partition, "ballot", this.LeaderBallot)

	leaderBallot := *this.LeaderBallot
	this.mutex.Unlock() // Unlock before outgoing messages with client

	this.client.Prepare(leaderBallot)

	wait := 50 * time.Millisecond
	for timeout > 0 {
		// Don't read leader without holding mutex!
		this.mutex.Lock()
		this.logger.Debug("Not yet leader", "id", this.Id, "partition", this.Partition, "leader", this.Leader, "timeout remaining", timeout.String(), "wait", wait.String())
		this.mutex.Unlock()

		time.Sleep(wait)

		timeout -= wait
		wait *= 2

		this.mutex.Lock()
		leader := this.Leader
		this.mutex.Unlock()

		if leader == this.Id {
			this.mutex.Lock()
			this.logger.Debug("TryTakeLeadership succeeded!", "id", this.Id, "partition", this.Partition)
			this.mutex.Unlock()

			return nil
		}
	}

	return errors.New("Could not take leadership within timeout!")
}

// Lock before calling
func (this *PaxosNode[TOp, TRes]) gainLeadership() {
	this.logger.Debug("Gained leadership", "id", this.Id, "partition", this.Partition)
	this.Leader = this.Id

	this.persistor.WriteSingleData(SingleData(&this.PaxosPersistentData))
}

func (this *PaxosNode[TOp, TRes]) IsLeader() bool {
	return this.Leader == this.Id
}

// Returns whether we lost leadership. Lock beforehand
func (this *PaxosNode[TOp, TRes]) promiseBallot(ballot Ballot) bool {
	this.logger.Debug("promiseBallot", "id", this.Id, "partition", this.Partition, "IsLeader", this.IsLeader(), "ballot", ballot, "previous", this.PromisedBallot)

	if this.PromisedBallot.GreaterThan(ballot) {
		return false // Never promise a lower ballot
	}

	old := this.PromisedBallot
	this.PromisedBallot = ballot
	err := this.persistor.WriteSingleData(SingleData(&this.PaxosPersistentData))
	if err != nil {
		this.PromisedBallot = old
		return false
	}

	if this.LeaderBallot != nil && this.PromisedBallot.GreaterThan(*this.LeaderBallot) {
		return true
	}

	return false
}

// Returns (success, acceptances, promised ballot)
func (this *PaxosNode[TOp, TRes]) ReceivePrepare(from string, ballot Ballot) (bool, []Slot[TOp], Ballot) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.PromisedBallot.GreaterThan(ballot) {
		this.logger.Debug("ReceivePrepare", "id", this.Id, "partition", this.Partition, "from", from, "ballot", ballot, "success", false, "acceptances", nil)
		return false, nil, this.PromisedBallot
	}

	this.promiseBallot(ballot)

	this.logger.Debug("ReceivePrepare", "id", this.Id, "partition", this.Partition, "from", from, "ballot", ballot, "success", true, "acceptances", this.Acceptances)

	acceptances := slices.Collect(maps.Values(this.Acceptances))
	if acceptances == nil {
		acceptances = []Slot[TOp]{}
	}
	return true, acceptances, this.PromisedBallot // Return a fixed snapshot of acceptances
}

func (this *PaxosNode[TOp, TRes]) ReceiveRequestAcceptance(from string, slot Slot[TOp]) (bool, Ballot) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	prevLeader := this.Leader
	this.Leader = from

	if prevLeader != this.Leader {
		err := this.persistor.WriteSingleData(SingleData(&this.PaxosPersistentData))
		if err != nil {
			return false, this.PromisedBallot
		}
	}

	if this.PromisedBallot.GreaterThan(slot.Ballot) {
		this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "partition", this.Partition, "from", from, "ballot", slot.Ballot, "slot", slot, "success", false, "reason", "Promised higher ballot")
		return false, this.PromisedBallot
	}

	if !this.isInPool(slot.Number, this.Id) {
		this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "partition", this.Partition, "from", from, "ballot", slot.Ballot, "slot", slot, "success", false, "reason", "Not in pool for that slot", "pool", this.GetPoolForSlot(slot.Number))
		return false, this.PromisedBallot
	}

	if slot.Number > this.HighestSlot {
		this.HighestSlot = slot.Number

		err := this.persistor.WriteSingleData(SingleData(&this.PaxosPersistentData))
		if err != nil {
			return false, this.PromisedBallot
		}
	}

	if this.persistor.AppendAcceptance(&slot) != nil {
		return false, this.PromisedBallot
	}
	this.Acceptances[slot.Number] = slot
	// Never accept anything below this ballot now
	this.promiseBallot(slot.Ballot)

	this.logger.Debug("ReceiveRequestAcceptance", "id", this.Id, "partition", this.Partition, "from", from, "ballot", slot.Ballot, "slot", slot, "success", true)

	return true, this.PromisedBallot
}

// Response from sending Prepare
func (this *PaxosNode[TOp, TRes]) ReceivePromise(from string, ballot Ballot, success bool, acceptances []Slot[TOp]) {
	this.mutex.Lock()

	this.logger.Debug("ReceivePromise", "id", this.Id, "partition", this.Partition, "from", from, "ballot", ballot, "success", success, "acceptances", acceptances)

	if !success {
		this.logger.Debug("ReceivePromise", "id", this.Id, "partition", this.Partition, "from", from, "accepted", false, "reason", "From did not promise successfully")
		this.mutex.Unlock() // Make sure to unlock!
		return
	}

	if this.LeaderBallot == nil {
		this.logger.Debug("ReceivePromise", "id", this.Id, "partition", this.Partition, "from", from, "accepted", false, "reason", "Not trying to claim leadership")
		this.mutex.Unlock()
		return
	}

	if ballot != *this.LeaderBallot {
		this.logger.Debug("ReceivePromise", "id", this.Id, "partition", this.Partition, "from", from, "accepted", false, "reason", "Promised ballot is not leader ballot", "leader ballot", this.LeaderBallot, "promised ballot", ballot)
		this.mutex.Unlock()
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

	if this.BallotPromisedBy.Intersect(this.LeadershipVoters).Cardinality() >= this.LeadershipVoters.Cardinality()/2+1 {
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
		this.PendingAcceptances[num] = PendingSlot[TOp]{
			Proposal:   val,
			AcceptedBy: mapset.NewSet[string](),
		}
		delete(this.PhaseOneAcceptances, num)
		this.RecoveredSlots.Add(num)

		toRequest = append(toRequest, val)
	}

	this.logger.Debug("ReceivePromise", "id", this.Id, "partition", this.Partition, "from", from, "accepted", true)

	this.mutex.Unlock()

	// Try to choose things already accepted
	for _, slot := range toRequest {
		go this.client.RequestAcceptance(slot)
	}
}

// Response from sending RequestAcceptance
func (this *PaxosNode[TOp, TRes]) ReceiveAccept(from string, ballot Ballot, success bool, slotIndex uint64) {
	this.mutex.Lock()

	this.logger.Debug("ReceiveAccept", "id", this.Id, "partition", this.Partition, "from", from, "ballot", ballot, "success", success, "slotIndex", slotIndex)

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

	this.logger.Debug("ReceiveCommit", "id", this.Id, "partition", this.Partition, "from", from, "slot", slot)

	this.Leader = from

	if slot.Number > this.HighestSlot {
		this.HighestSlot = slot.Number
	}

	delete(this.PendingAcceptances, slot.Number)
	this.Chosen[slot.Number] = ChosenSlot[TOp]{
		Number:     slot.Number,
		Type:       slot.Type,
		Operation:  slot.Operation,
		PoolChange: slot.PoolChange,
	}
	
	this.persistor.AppendChosen(&slot.ChosenSlot)
}

// Blocks until value is committed. Also see ProposePoolChange and ProposeNoOp
func (this *PaxosNode[TOp, TRes]) Propose(val TOp) (uint64, error) {
	return this.ProposeWithTimeout(ChosenSlotOperation, val, TIMEOUT)
}

// Blocks until value is committed.
func (this *PaxosNode[TOp, TRes]) ProposePoolChange(op PoolChange, id string) (uint64, error) {
	return this.ProposeWithTimeout(ChosenSlotPoolChange, PoolChangeOp{
		Op: op,
		Id: id,
	}, TIMEOUT)
}

func (this *PaxosNode[TOp, TRes]) ProposeNoOp() (uint64, error) {
	return this.ProposeWithTimeout(ChosenSlotNoOp, nil, TIMEOUT)
}

// Returns (selected slot, error)
func (this *PaxosNode[TOp, TRes]) ProposeWithTimeout(slotType ChosenSlotType, val any, timeout time.Duration) (uint64, error) {
	this.mutex.Lock()

	slotNumber := this.HighestSlot + 1

	this.logger.Debug("Proposing...", "slot", slotNumber, "type", slotType, "val", val, "timeout", timeout)

	ballot := *this.LeaderBallot

	slot := Slot[TOp]{
		Number: slotNumber,
		Ballot: ballot,
		Type:   slotType,
	}

	switch slotType {
	case ChosenSlotOperation:
		casted := val.(TOp)
		slot.Operation = casted
	case ChosenSlotPoolChange:
		casted := val.(PoolChangeOp)
		slot.PoolChange = &casted
	}

	this.PendingAcceptances[slotNumber] = PendingSlot[TOp]{
		Proposal:   slot,
		AcceptedBy: mapset.NewSet[string](),
	}

	this.mutex.Unlock() // Unlock while call runs
	go this.client.RequestAcceptance(slot)

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

	this.logger.Debug("Something was chosen", "chosen", chosen, "type", slotType, "val", val, "slot", slotNumber, "equal", chosen.equal(slotType, val))
	if !chosen.equal(slotType, val) {
		// Something else was chosen try again if we have time left
		if timeout < 0 {
			return 0, errors.New("Something else was chosen and no time was left to retry")
		}

		return this.ProposeWithTimeout(slotType, val, timeout)
	}

	// Success!
	return slotNumber, nil
}

func (this *PaxosNode[TOp, TRes]) GetState() TRes {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	var res TRes

	expectedSlot := uint64(0)
	// +1 so we process the last slot
	for slot := range this.HighestSlot + 1 {
		if slot > expectedSlot {
			break
		}
		expectedSlot++

		val := this.Chosen[slot]
		if val.Type == ChosenSlotOperation {
			res = this.ProcessOperation(val.Operation, res)
		}
	}

	return res
}
