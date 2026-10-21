package paxocache

import (
	"fmt"
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

type PaxosClient[T comparable] interface {
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

type Slot[T comparable] struct {
	ChosenSlot[T]
	// Highest ballot this slot was accepted with
	Ballot Ballot
}
 
type PendingSlot[T comparable] struct {
	Proposal Slot[T]
	AcceptedBy mapset.Set[string]
}

type ChosenSlot[T comparable] struct {
	Number uint64
	Type ChosenSlotType
	Operation *T `json:"Operation,omitempty"`
	PoolChange *PoolChangeOp `json:"PoolChange,omitempty"`
}

func (this *ChosenSlot[T]) equal(slotType ChosenSlotType, val any) bool {
	if this.Type != slotType {
		return false
	}

	if this.Type == ChosenSlotOperation {
		return *this.Operation == val.(T)
	}

	if this.Type == ChosenSlotPoolChange {
		return *this.PoolChange == val.(PoolChangeOp)
	}

	// NoOps are always equal
	return true
}

type ChosenSlotType string
const (
	ChosenSlotOperation = "operation"
	ChosenSlotPoolChange = "pool_change"
	ChosenSlotNoOp = "no_op"
)