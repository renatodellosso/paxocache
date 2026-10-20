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

	poolA := paxosA.GetRootPool()
	poolB := paxosB.GetRootPool()

	assert.NotNilf(t, poolA, "poolA is nil (could not be determined)!")
	assert.ElementsMatch(t, []string{discoveryA.Address, discoveryB.Address}, poolA.ToSlice())
	assert.NotNilf(t, poolB, "poolB is nil (could not be determined)!")
	assert.ElementsMatch(t, []string{discoveryA.Address, discoveryB.Address}, poolB.ToSlice())

	assert.Equal(t, discoveryA.Address, paxosA.GetRootLeader())
	assert.Equal(t, discoveryA.Address, paxosB.GetRootLeader())
}

func TestBootstrapFromExistingBootstrapsPartitions(t *testing.T) {
	logger := tests.SilentLogger()

	_, discoveryA, paxosA := createTestStack(logger)
	_, discoveryB, paxosB := createTestStack(logger)

	partition := "test"

	assert.Nilf(t, paxosA.BootstrapCluster(), "paxosA failed to bootstrap cluster!")
	assert.Nilf(t, paxosA.BootstrapPartitionForCluster(partition), "Failed to bootstrap partition for cluster!")

	assert.Nilf(t, discoveryB.ExchangeWith(discoveryA.Address, true), "discoveryB failed to exchange with discoveryA!")

	assert.Nilf(t, paxosB.BootstrapFromExisting(), "paxosB failed to bootstrap from existing!")

	assert.ElementsMatch(t, []string{partition}, paxosB.GetPartitions().ToSlice())
}

func TestBootstrapPartitionFromExistingWorks(t *testing.T) {
	logger := tests.SilentLogger()

	_, discoveryA, paxosA := createTestStack(logger)
	_, discoveryB, paxosB := createTestStack(logger)

	assert.Nilf(t, paxosA.BootstrapCluster(), "paxosA failed to bootstrap cluster!")
	assert.Nilf(t, discoveryB.ExchangeWith(discoveryA.Address, true), "discoveryB failed to exchange with discoveryA!")

	assert.Nilf(t, paxosB.BootstrapFromExisting(), "paxosB failed to bootstrap from existing!")

	partition := "test"

	assert.Nilf(t, paxosA.BootstrapPartitionForCluster(partition), "Failed to bootstrap partition for cluster!")
	assert.Nilf(t, paxosB.BootstrapPartitionFromExisting(partition), "Failed to bootstrap partition from existing!")

	assert.ElementsMatch(t, []string{partition}, paxosB.GetPartitions().ToSlice())
}