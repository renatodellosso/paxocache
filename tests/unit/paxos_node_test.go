package unit

import (
	"maps"
	paxocache "paxocache/src"
	"paxocache/tests"
	"slices"
	"testing"
	"testing/synctest"

	mapset "github.com/deckarep/golang-set/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockPaxosClient[T comparable] struct {
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

func (this *MockPaxosClient[T]) Join(from string) error {
	this.Called(from)
	return nil
}

func (this *MockPaxosClient[T]) Propose(val T) error {
	this.Called(val)
	return nil
}

func TestNewPaxosNodeSetsValues(t *testing.T) {
	var client paxocache.PaxosClient[any]
	logger := tests.SilentLogger()
	id := "id-123"

	node := paxocache.NewPaxosNode[any, any](id, "", client, logger, nil, mapset.NewSet(id))

	assert.Equal(t, id, node.Id)

	assert.False(t, node.IsLeader())
	assert.Equal(t, uint64(paxocache.POOL_DELAY - 1), node.HighestSlot)

	assert.Nil(t, node.LeaderBallot)
	assert.Equal(t, paxocache.Ballot{
		Number: 0,
		OwnerId: "",
	}, node.PromisedBallot)
	
	assert.Equal(t, mapset.NewSet[string](), node.BallotPromisedBy)

	assert.Equal(t, 0, len(node.PhaseOneAcceptances))
	assert.Equal(t, 0, len(node.Acceptances))
	assert.Equal(t, 0, len(node.PendingAcceptances))
	assert.Equal(t, paxocache.POOL_DELAY, len(node.Chosen))
}

func TestGetPoolForSlotReturnsSelfForStarterPools(t *testing.T) {
	var client paxocache.PaxosClient[any]
	logger := tests.SilentLogger()
	id := "id-123"

	node := paxocache.NewPaxosNode[any, any](id, "", client, logger, nil, mapset.NewSet(id))

	expected := []string{id}
	for i := range uint64(paxocache.POOL_DELAY) {
		res := node.GetPoolForSlot(i)
		assert.NotNilf(t, res, "%s has no pool!", i)
		assert.ElementsMatch(t, expected, res.ToSlice())
	}
}

func TestGetPoolForSlotAppliesPoolChanges(t *testing.T) {
	var client paxocache.PaxosClient[any]
	logger := tests.SilentLogger()
	id := "id-123"

	node := paxocache.NewPaxosNode[any, any](id, "", client, logger, nil, mapset.NewSet(id))

	// Set slots 0 and 1 so changes take effect for POOL_DELAY and POOL_DELAY + 1
	node.Chosen[0] = paxocache.ChosenSlot[any]{
		Number: 0,
		Type: paxocache.ChosenSlotPoolChange,
		PoolChange: &paxocache.PoolChangeOp{
			Op: paxocache.PoolChangeAdd,
			Id: "A",
		},
	}
	node.Chosen[1] = paxocache.ChosenSlot[any]{
		Number: 1,
		Type: paxocache.ChosenSlotPoolChange,
		PoolChange: &paxocache.PoolChangeOp{
			Op: paxocache.PoolChangeRemove,
			Id: "A",
		},
	}

	res := node.GetPoolForSlot(paxocache.POOL_DELAY)
	assert.NotNilf(t, res, "Res is nil!")
	assert.ElementsMatch(t, []string{node.Id, "A"}, res.ToSlice())

	res = node.GetPoolForSlot(paxocache.POOL_DELAY + 1)
	assert.NotNilf(t, res, "Res is nil!")
	assert.ElementsMatch(t, []string{node.Id}, res.ToSlice())
}

func TestTakeLeadershipTakesLeadership(t *testing.T) {
	// Using synctest makes Time.sleep use fake time instead of real time
	synctest.Test(t, func(t *testing.T) {
		client := new(MockPaxosClient[any])

		node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))

		client.On("Prepare", paxocache.Ballot{
			Number: 1,
			OwnerId: node.Id,
		}).Run(func(args mock.Arguments) {
			success, acceptances, promisedBallot := node.ReceivePrepare(node.Id, args.Get(0).(paxocache.Ballot))
			node.ReceivePromise(node.Id, promisedBallot, success, acceptances)
		})

		node.TryTakeLeadership(paxocache.TIMEOUT)

		assert.Equal(t, &paxocache.Ballot{
			Number: 1,
			OwnerId: node.Id,
		}, node.LeaderBallot)
		assert.Equal(t, node.Id, node.Leader)
		assert.ElementsMatch(t, []string{node.Id}, node.BallotPromisedBy.ToSlice())
	})
}

func TestReceivePrepareFailsIfAHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))
	
	ballot := paxocache.Ballot{
		Number: 1,
		OwnerId: "A",
	}

	// Same number, higher owner ID
	node.PromisedBallot = paxocache.Ballot{
		Number: 1,
		OwnerId: "B",
	}

	success, acceptances, _ := node.ReceivePrepare(ballot.OwnerId, ballot)

	assert.False(t, success)
	assert.Nil(t, acceptances)

	// Higher number, same owner ID
	node.PromisedBallot = paxocache.Ballot{
		Number: 2,
		OwnerId: "A",
	}

	success, acceptances, _ = node.ReceivePrepare(ballot.OwnerId, ballot)

	assert.False(t, success)
	assert.Nil(t, acceptances)
}

func TestReceivePrepareSucceedsIfNoHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))
	
	ballot := paxocache.Ballot{
		Number: 2,
		OwnerId: "A",
	}

	node.PromisedBallot = paxocache.Ballot{
		Number: 1,
		OwnerId: "B",
	}

	success, acceptances, _ := node.ReceivePrepare(ballot.OwnerId, ballot)

	assert.True(t, success)
	assert.NotNil(t, acceptances)
}

func TestReceivePrepareReturnsAcceptancesOnSuccess(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))
	
	node.PromisedBallot = paxocache.Ballot{
		Number: 1,
		OwnerId: "B",
	}
	node.Acceptances[1] = paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 1,
			Type: paxocache.ChosenSlotOperation,
			Operation: new(any(123)),
		},
		Ballot: node.PromisedBallot,
	}

	ballot := paxocache.Ballot{
		Number: 2,
		OwnerId: "A",
	}

	_, acceptances, _ := node.ReceivePrepare(ballot.OwnerId, ballot)

	assert.Equal(t, slices.Collect(maps.Values(node.Acceptances)), acceptances)
}

func TestReceiveRequestAcceptanceRejectsIfAHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))

	ballot := paxocache.Ballot{
		Number: 2,
		OwnerId: node.Id,
	}
	node.PromisedBallot = ballot

	success, _ := node.ReceiveRequestAcceptance(node.Id, paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 0,
			Operation: new(any(123)),
		},
		Ballot: paxocache.Ballot{
			Number: 1,
			OwnerId: node.Id,
		},
	})

	assert.False(t, success)
	
	// Make sure no changes were made
	assert.Equal(t, 0, len(node.Acceptances))
	assert.Equal(t, ballot, node.PromisedBallot)
}

func TestReceiveRequestAcceptanceSucceedsIfNoHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))

	ballot := paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	}
	node.PromisedBallot = ballot

	success, _ := node.ReceiveRequestAcceptance(node.Id, paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 0,
			Operation: new(any(123)),
		},
		Ballot: paxocache.Ballot{
			Number: 2,
			OwnerId: node.Id,
		},
	})

	assert.True(t, success)
}

func TestReceiveRequestAcceptancePromisesBallotIfNoHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))

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
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 0,
			Operation: new(any(123)),
		},
		Ballot: newBallot,
	})

	assert.Equal(t, newBallot, node.PromisedBallot)
}

func TestReceiveRequestAcceptanceAcceptsValueIfNoHigherBallotHasBeenPromised(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))

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
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 0,
			Operation: new(any(123)),
		},
		Ballot: newBallot,
	}

	node.ReceiveRequestAcceptance(node.Id, slot)

	val, ok := node.Acceptances[slot.Number]

	assert.True(t, ok)
	assert.Equal(t, slot, val)
}

func TestReceivePromiseDoesNothingOnFailure(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))

	node.ReceivePromise(node.Id, paxocache.Ballot{
		Number: 0,
		OwnerId: node.Id,
	}, false, []paxocache.Slot[any]{})

	assert.Equal(t, 0, node.BallotPromisedBy.Cardinality())
}

func TestReceivePromiseDoesNothingOnOlderBallot(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))

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
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))
	node.HighestSlot = 99 // Don't gain leadership

	node.Pools[1] = mapset.NewSet(node.Id, "other1", "other2")
	node.Pools[2] = mapset.NewSet(node.Id, "other1", "other2")
	node.Pools[3] = mapset.NewSet(node.Id, "other1", "other2")

	ballot := paxocache.Ballot{
		Number: 1,
		OwnerId: node.Id,
	}
	node.LeaderBallot = &ballot

	slotA := paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 1,
			Operation: new(any(123)),
		},
		Ballot: ballot,
	}
	slotB := paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 2,
			Operation: new(any(456)),
		},
		Ballot: paxocache.Ballot{},
	}
	slotC := paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 3,
			Operation: new(any(789)),
		},
		Ballot: paxocache.Ballot{},
	}

	node.PhaseOneAcceptances[slotA.Number] = slotA
	node.PhaseOneAcceptances[slotB.Number] = slotB

	node.ReceivePromise(node.Id, ballot, true, []paxocache.Slot[any]{slotB, slotC, {
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 1,
			Operation: new(any(-123)),
		},
		Ballot: paxocache.Ballot{},
	}})

	assert.Equal(t, 3, len(node.PhaseOneAcceptances))
	assert.Equal(t, slotA, node.PhaseOneAcceptances[slotA.Number])
	assert.Equal(t, slotB, node.PhaseOneAcceptances[slotB.Number])
	assert.Equal(t, slotC, node.PhaseOneAcceptances[slotC.Number])
}

func TestReceiveAcceptAddsToAcceptedBy(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))
	node.HighestSlot = 99 // Don't gain leadership

	node.Pools[1] = mapset.NewSet(node.Id, "other")

	slot := paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 1,
			Operation: new(any(123)),
		},
		Ballot: paxocache.Ballot{},
	}
	node.PendingAcceptances[slot.Number] = paxocache.PendingSlot[any]{
		Proposal: slot,
		AcceptedBy: mapset.NewSet[string](),
	}

	node.ReceiveAccept(node.Id, slot.Ballot, true, slot.Number)

	assert.ElementsMatch(t, []string{node.Id}, node.PendingAcceptances[slot.Number].AcceptedBy.ToSlice())
}

func TestReceiveAcceptCommitsOnceQuorumIsReached(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))
	node.HighestSlot = 99 // Don't gain leadership

	node.Pools[1] = mapset.NewSet(node.Id)

	slot := paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 1,
			Operation: new(any(123)),
		},
		Ballot: paxocache.Ballot{},
	}

	client.On("Commit", slot)

	node.PendingAcceptances[slot.Number] = paxocache.PendingSlot[any]{
		Proposal: slot,
		AcceptedBy: mapset.NewSet[string](),
	}

	node.ReceiveAccept(node.Id, slot.Ballot, true, slot.Number)

	client.AssertExpectations(t)
}

func TestReceiveAcceptCommitsIfQuorumHasAccepted(t *testing.T) {
	client := new(MockPaxosClient[any])
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))
	node.HighestSlot = 99 // Don't gain leadership

	slot := paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 0,
			Operation: new(any(123)),
		},
		Ballot: paxocache.Ballot{},
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
	node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))

	slot := paxocache.Slot[any]{
		ChosenSlot: paxocache.ChosenSlot[any]{
			Number: 0,
			Operation: new(any(123)),
		},
		Ballot: paxocache.Ballot{},
	}

	node.ReceiveCommit(node.Id, slot)

	assert.Equal(t, paxocache.ChosenSlot[any]{
		Number: slot.Number,
		Operation: slot.Operation,
	}, node.Chosen[slot.Number])
}

func TestProposeWithTimeoutRequestsAcceptance(t *testing.T) {
	// Using synctest makes Time.sleep use fake time instead of real time
	synctest.Test(t, func(t *testing.T) {
		client := new(MockPaxosClient[any])
		node := paxocache.NewPaxosNode[any, any]("id", "", client, tests.SilentLogger(), nil, mapset.NewSet("id"))

		node.LeaderBallot = &paxocache.Ballot{
			Number: 0,
			OwnerId: node.Id,
		}
		node.Leader = node.Id

		val := 1
		slot := paxocache.Slot[any]{
			Number: node.HighestSlot + 1,
			Type: paxocache.ChosenSlotOperation,
			Operation: new(any(val)),
			Ballot: *node.LeaderBallot,
		}

		client.On("RequestAcceptance", slot).Run(func(args mock.Arguments) {
			node.ReceiveCommit(node.Id, args.Get(0).(paxocache.Slot[any]))
		})

		var slotIndex uint64
		var err error

		slotIndex, err = node.ProposeWithTimeout(paxocache.ChosenSlotOperation, val, paxocache.TIMEOUT)

		client.AssertExpectations(t)

		assert.Nil(t, err)
		assert.Equal(t, slot.Number, slotIndex)
	})
}