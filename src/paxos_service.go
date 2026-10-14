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

// Handles Multipaxos for one log
type PaxosService[T any] struct {
	Id string

	communicator PaxosCommunicator[T]
	logger *slog.Logger

	// Have we successfully obtained a quorum for its ballot and is not aware of being superseded yet?
	IsLeader bool
	// Ballot we're using to claim leadership
	LeaderBallot *Ballot
	LeaderBallotPromisedBy mapset.Set[string]

	PromisedBallot Ballot
	Acceptances map[uint64]Slot[T]
	Chosen map[uint64]Slot[T]
}

type PaxosCommunicator[T any] interface {
	Prepare(Ballot)
	RequestAcceptance(Slot[T])
	Commit(Slot[T])
}

func (this *PaxosService[T]) Promise(from string, ballot Ballot, success bool, acceptances []Slot[T]) {

}

func (this *PaxosService[T]) Accept(from string, ballot Ballot, success bool, slotIndex uint64) {
	
}