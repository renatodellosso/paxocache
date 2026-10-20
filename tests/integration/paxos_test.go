package integration

import (
	"log/slog"
	paxocache "paxocache/src"
	"paxocache/tests"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createTestStack(logger *slog.Logger) (*paxocache.Listener, *paxocache.ServerDiscoveryService, *paxocache.Paxos[any, any]) {
	listener := paxocache.Listener{
		Logger: logger,
		Token: "",
	}
	listener.Listen()
	
	discovery := paxocache.NewServerDiscoveryService(tests.SilentLogger())
	discovery.RegisterListener(&listener)

	paxos := paxocache.NewPaxos[any, any](logger, discovery)
	paxos.RegisterListener(&listener)

	return &listener, discovery, paxos
}

func TestBootstrapClusterWorksForSingleNodeCluster(t *testing.T) {
	// Don't use synctest unless we mock networking

	logger := tests.SilentLogger()
	_, _, paxos := createTestStack(logger)

	partition := "test"

	err := paxos.BootstrapCluster()
	assert.Nilf(t, err, "Err is not nil after bootstrapping cluster!")

	err = paxos.BootstrapPartitionForCluster(partition)

	assert.Nilf(t, err, "Err is not nil after bootstrapping partition!")

	partitions := paxos.GetPartitions()
	assert.NotNilf(t, partitions, "Partitions is nil!")
	assert.ElementsMatch(t, []string{partition}, partitions.ToSlice())
}

func TestBootstrapFromExistingWorks(t *testing.T) {
	logger := tests.SilentLogger()

	_, discoveryA, paxosA := createTestStack(logger)
	_, discoveryB, paxosB := createTestStack(logger)

	assert.Nilf(t, paxosA.BootstrapCluster(), "paxosA failed to bootstrap cluster!")
	assert.Nilf(t, discoveryB.ExchangeWith(discoveryA.Address, true), "discoveryB failed to exchange with discoveryA!")

	assert.Nilf(t, paxosB.BootstrapFromExisting(), "paxosB failed to bootstrap from existing!")

	poolA := paxosA.GetRootPool().ToSlice()
	poolB := paxosA.GetRootPool().ToSlice()

	assert.ElementsMatch(t, []string{discoveryA.Address, discoveryB.Address}, poolA)
	assert.ElementsMatch(t, []string{discoveryA.Address, discoveryB.Address}, poolB)
}