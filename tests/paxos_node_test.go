package tests

import (
	paxocache "paxocache/src"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPaxosNodeSetsValues(t *testing.T) {
	var client paxocache.PaxosClient[any]
	logger := SilentLogger()
	id := "id-123"

	node := paxocache.NewPaxosNode(id, client, logger)

	assert.Equal(t, id, node.Id)

	assert.False(t, node.IsLeader)
	assert.Equal(t, uint64(0), node.HighestSlot)

	assert.Nil(t, node.LeaderBallot)
	assert.Nil(t, node.PromisedBallot)
	
	assert.Equal(t, 0, len(node.BallotPromisedBy))
	assert.Equal(t, 0, len(node.PhaseOneAcceptances))
	assert.Equal(t, 0, len(node.Acceptances))
	assert.Equal(t, 0, len(node.PendingAcceptances))
	assert.Equal(t, 0, len(node.Chosen))
}