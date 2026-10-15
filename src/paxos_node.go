package paxocache

import (
	"log/slog"

	mapset "github.com/deckarep/golang-set/v3"
)

// Should be comparable by default
type Ballot struct {
	Number uint64
	OwnerId string
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
	BallotPromisedBy map[Ballot]mapset.Set[string]
	// Acceptances we've seen while taking leadership
	PhaseOneAcceptances map[uint64]Slot[T]

	// Highest ballot we've promised
	PromisedBallot *Ballot

	// Highest slot index we've seen or promised
	HighestSlot uint64

	// What we've accepted, maps slot index -> highest balloted value
	Acceptances map[uint64]Slot[T]
	// Acceptances currently pending
	PendingAcceptances map[uint64]PendingSlot[T]
	// Slots already chosen
	Chosen map[uint64]ChosenSlot[T]
}

type PaxosClient[T any] interface {
	Prepare(Ballot)
	RequestAcceptance(Slot[T])
	Commit(Slot[T])
}

func NewPaxosNode[T any](id string, client PaxosClient[T], logger *slog.Logger) *PaxosNode[T] {
	return &PaxosNode[T]{
		Id: id,

		client: client,
		logger: logger,

		BallotPromisedBy: make(map[Ballot]mapset.Set[string]),
		PhaseOneAcceptances: make(map[uint64]Slot[T]),

		Acceptances: make(map[uint64]Slot[T]),
		PendingAcceptances: make(map[uint64]PendingSlot[T]),
		Chosen: make(map[uint64]ChosenSlot[T]),
	}
}

func (this *PaxosNode[T]) QuorumSize() uint64 {
	return this.PoolSize / 2 + 1
}

func (this *PaxosNode[T]) ReceivePrepare(from string, ballot Ballot) (bool, []Slot[T]) {
	return false, nil
}

func (this *PaxosNode[T])  ReceiveRequestAcceptance(from string, slot Slot[T]) bool {
	return false
}

func (this *PaxosNode[T]) ReceivePromise(from string, ballot Ballot, success bool, acceptances []Slot[T]) {

}

func (this *PaxosNode[T]) ReceiveAccept(from string, ballot Ballot, success bool, slotIndex uint64) {
	
}

func (this *PaxosNode[T]) ReceiveCommit(from string, slot Slot[T]) {

}