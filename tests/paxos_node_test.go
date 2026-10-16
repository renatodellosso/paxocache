package tests

import (
	paxocache "paxocache/src"
	"testing"

	mapset "github.com/deckarep/golang-set/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockPaxosClient[T any] struct {
	// Struct embedding
	mock.Mock
}

func (this *MockPaxosClient[T]) Prepare(ballot paxocache.Ballot) {
	this.Called(ballot)
}

func (this *MockPaxosClient[T]) RequestAcceptance(slot paxocache.Slot[T]) {
	this.Called(slot)
}

func (this *MockPaxosClient[T]) Commit(slot paxocache.Slot[T]) {
	this.Called(slot)
}

func TestNewPaxosNodeSetsValues(t *testing.T) {
	var client paxocache.PaxosClient[any]
	logger := SilentLogger()
	id := "id-123"

	node := paxocache.NewPaxosNode(id, "", client, logger)

	assert.Equal(t, id, node.Id)

	assert.False(t, node.IsLeader)
	assert.Equal(t, uint64(0), node.HighestSlot)

	assert.Nil(t, node.LeaderBallot)
	assert.Equal(t, paxocache.Ballot{
		Number: 0,
		OwnerId: "",
	}, node.PromisedBallot)
	
	assert.Equal(t, mapset.NewSet[string](), node.BallotPromisedBy)

	assert.Equal(t, 0, len(node.PhaseOneAcceptances))
	assert.Equal(t, 0, len(node.Acceptances))
	assert.Equal(t, 0, len(node.PendingAcceptances))
	assert.Equal(t, 0, len(node.Chosen))
}

func TestTakeLeadershipSetsLeaderBallot(t *testing.T) {
	client := new(MockPaxosClient[any])

	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	client.On("Prepare", paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	})

	node.TryTakeLeadership()

	assert.Equal(t, &paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	}, node.LeaderBallot)
}

func TestTryTakeLeadershipCallsPrepare(t *testing.T) {
	client := new(MockPaxosClient[any])

	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	client.On("Prepare", paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	})

	node.TryTakeLeadership()
	
	client.AssertExpectations(t)
}

func TestGainLeadershipUpdatesState(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	node.GainLeadership()

	assert.True(t, node.IsLeader)
	assert.Equal(t, 0, len(node.PhaseOneAcceptances))
}

func TestGainLeadershipConvertsPhaseOneAcceptances(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())
	
	ballot := paxocache.Ballot{
		Number: 0,
		OwnerId: node.Id,
	}
	node.LeaderBallot = &ballot

	slotA := paxocache.Slot[any]{
		Number: 0,
		Ballot: ballot,
		Val: 123,
	}
	node.PhaseOneAcceptances[slotA.Number] = slotA

	slotB := paxocache.Slot[any]{
		Number: 1,
		Ballot: ballot,
		Val: 456,
	}
	node.PhaseOneAcceptances[slotB.Number] = slotB

	client.On("RequestAcceptance", slotA)
	client.On("RequestAcceptance", slotB)

	node.GainLeadership()

	client.AssertExpectations(t)

	assert.Equal(t, 0, len(node.PhaseOneAcceptances))
	assert.Equal(t, 2, len(node.PendingAcceptances))

	assert.Equal(t, slotA, node.PendingAcceptances[slotA.Number].Proposal)
	assert.Equal(t, slotB, node.PendingAcceptances[slotB.Number].Proposal)
}

func TestReceivePrepareFailsIfAHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())
	
	ballot := paxocache.Ballot{
		Number: 1,
		OwnerId: "A",
	}

	// Same number, higher owner ID
	node.PromisedBallot = paxocache.Ballot{
		Number: 1,
		OwnerId: "B",
	}

	success, acceptances := node.ReceivePrepare(ballot.OwnerId, ballot)

	assert.False(t, success)
	assert.Nil(t, acceptances)

	// Higher number, same owner ID
	node.PromisedBallot = paxocache.Ballot{
		Number: 2,
		OwnerId: "A",
	}

	success, acceptances = node.ReceivePrepare(ballot.OwnerId, ballot)

	assert.False(t, success)
	assert.Nil(t, acceptances)
}

func TestReceivePrepareSucceedsIfNoHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())
	
	ballot := paxocache.Ballot{
		Number: 2,
		OwnerId: "A",
	}

	node.PromisedBallot = paxocache.Ballot{
		Number: 1,
		OwnerId: "B",
	}

	success, acceptances := node.ReceivePrepare(ballot.OwnerId, ballot)

	assert.True(t, success)
	assert.NotNil(t, acceptances)
}

func TestReceivePrepareReturnsAcceptancesOnSuccess(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())
	
	node.PromisedBallot = paxocache.Ballot{
		Number: 1,
		OwnerId: "B",
	}
	node.Acceptances[1] = paxocache.Slot[any]{
		Number: 1,
		Ballot: node.PromisedBallot,
		Val: 123,
	}

	ballot := paxocache.Ballot{
		Number: 2,
		OwnerId: "A",
	}

	_, acceptances := node.ReceivePrepare(ballot.OwnerId, ballot)

	assert.Equal(t, node.Acceptances, acceptances)
}

func TestReceiveRequestAcceptanceRejectsIfAHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	ballot := paxocache.Ballot{
		Number: 2,
		OwnerId: node.Id,
	}
	node.PromisedBallot = ballot

	success := node.ReceiveRequestAcceptance(node.Id, paxocache.Slot[any]{
		Ballot: paxocache.Ballot{
			Number: 1,
			OwnerId: node.Id,
		},
		Number: 0,
		Val: 123,
	})

	assert.False(t, success)
	
	// Make sure no changes were made
	assert.Equal(t, 0, len(node.Acceptances))
	assert.Equal(t, ballot, node.PromisedBallot)
}

func TestReceiveRequestAcceptanceSucceedsIfNoHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	ballot := paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	}
	node.PromisedBallot = ballot

	success := node.ReceiveRequestAcceptance(node.Id, paxocache.Slot[any]{
		Ballot: paxocache.Ballot{
			Number: 2,
			OwnerId: node.Id,
		},
		Number: 0,
		Val: 123,
	})

	assert.True(t, success)
}

func TestReceiveRequestAcceptancePromisesBallotIfNoHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	ballot := paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	}
	node.PromisedBallot = ballot

	newBallot := paxocache.Ballot{
		Number: 2,
		OwnerId: node.Id,
	}

	node.ReceiveRequestAcceptance(node.Id, paxocache.Slot[any]{
		Ballot: newBallot,
		Number: 0,
		Val: 123,
	})

	assert.Equal(t, newBallot, node.PromisedBallot)
}

func TestReceiveRequestAcceptanceAcceptsValueIfNoHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	ballot := paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	}
	node.PromisedBallot = ballot

	newBallot := paxocache.Ballot{
		Number: 2,
		OwnerId: node.Id,
	}
	slot := paxocache.Slot[any]{
		Ballot: newBallot,
		Number: 0,
		Val: 123,
	}

	node.ReceiveRequestAcceptance(node.Id, slot)

	val, ok := node.Acceptances[slot.Number]

	assert.True(t, ok)
	assert.Equal(t, slot, val)
}

func TestReceivePromiseDoesNothingOnFailure(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	node.ReceivePromise(node.Id, paxocache.Ballot{
		Number: 0,
		OwnerId: node.Id,
	}, false, []paxocache.Slot[any]{})

	assert.Equal(t, 0, node.BallotPromisedBy.Cardinality())
}

func TestReceivePromiseDoesNothingOnOlderBallot(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	node.PromisedBallot = paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	}

	node.ReceivePromise(node.Id, paxocache.Ballot{
		Number: 0,
		OwnerId: node.Id,
	}, false, []paxocache.Slot[any]{})

	assert.Equal(t, 0, node.BallotPromisedBy.Cardinality())
}

func TestReceivePromiseSetsPhaseOneAcceptancesOnSuccess(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())
	node.PoolSize = 99 // Ensure we don't wipe phase one acceptances by gaining leadership

	ballot := paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	}
	node.LeaderBallot = &ballot

	slotA := paxocache.Slot[any]{
		Number: 0,
		Ballot: ballot,
		Val: 123,
	}
	slotB := paxocache.Slot[any]{
		Number: 1,
		Ballot: paxocache.Ballot{},
		Val: 456,
	}
	slotC := paxocache.Slot[any]{
		Number: 2,
		Ballot: paxocache.Ballot{},
		Val: 789,
	}

	node.PhaseOneAcceptances[slotA.Number] = slotA
	node.PhaseOneAcceptances[slotB.Number] = slotB

	node.ReceivePromise(node.Id, ballot, true, []paxocache.Slot[any]{slotB, slotC, {
		Number: 0,
		Ballot: paxocache.Ballot{},
		Val: -123,
	}})

	assert.Equal(t, 3, len(node.PhaseOneAcceptances))
	assert.Equal(t, slotA, node.PhaseOneAcceptances[slotA.Number])
	assert.Equal(t, slotB, node.PhaseOneAcceptances[slotB.Number])
	assert.Equal(t, slotC, node.PhaseOneAcceptances[slotC.Number])
}

func TestReceiveAcceptAddsToAcceptedBy(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())
	node.PoolSize = 99 // Ensure we don't commit

	slot := paxocache.Slot[any]{
		Number: 0,
		Ballot: paxocache.Ballot{},
		Val: 123,
	}
	node.PendingAcceptances[slot.Number] = paxocache.PendingSlot[any]{
		Proposal: slot,
		AcceptedBy: mapset.NewSet[string](),
	}

	node.ReceiveAccept(node.Id, slot.Ballot, true, slot.Number)

	assert.ElementsMatch(t, []string{node.Id}, node.PendingAcceptances[slot.Number].AcceptedBy.ToSlice())
}

func TestReceiveAcceptCommitsIfQuorumHasAccepted(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())
	node.PoolSize = 1 // Ensure we don't commit

	slot := paxocache.Slot[any]{
		Number: 0,
		Ballot: paxocache.Ballot{},
		Val: 123,
	}
	node.PendingAcceptances[slot.Number] = paxocache.PendingSlot[any]{
		Proposal: slot,
		AcceptedBy: mapset.NewSet[string](),
	}

	client.On("Commit", slot)

	node.ReceiveAccept(node.Id, slot.Ballot, true, slot.Number)

	client.AssertExpectations(t)
}

func TestReceiveCommitAddsToChosen(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode("id", "", client, SilentLogger())

	slot := paxocache.Slot[any]{
		Number: 0,
		Ballot: paxocache.Ballot{},
		Val: 123,
	}

	node.ReceiveCommit(node.Id, slot)

	assert.Equal(t, paxocache.ChosenSlot[any]{
		Number: slot.Number,
		Val: slot.Val,
	}, node.Chosen[slot.Number])
}