package integration

import (
	"log/slog"
	paxocache "paxocache/src"
	"paxocache/tests"
	"testing"

	mapset "github.com/deckarep/golang-set/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestStack(logger *slog.Logger) (*paxocache.Listener, *paxocache.ServerDiscoveryService, *paxocache.Paxos[any, any]) {
	listener := paxocache.Listener{
		Logger: logger,
		Token: "",
	}
	listener.Listen()
	
	discovery := paxocache.NewServerDiscoveryService(tests.SilentLogger())
	discovery.RegisterListener(&listener)

	createPartitionPersistor := func(partition string) paxocache.PaxosPersistor[any, any] {
		return &paxocache.NoOpPersistor[any, any]{}
	}

	createRootPersistor := func() paxocache.PaxosPersistor[paxocache.PartitionOperation, mapset.Set[string]] {
		return &paxocache.NoOpPersistor[paxocache.PartitionOperation, mapset.Set[string]]{}
	}

	paxos := paxocache.NewPaxos(logger, discovery, createRootPersistor, createPartitionPersistor)
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

func TestBootstrapClusterRecoversData(t *testing.T) {
	logger := tests.SilentLogger()
	_, _, paxos := createTestStack(logger)

	persistor, create := tests.CreateMockRootPersistor()
	paxos.CreateRootPersistor = create

	data := paxocache.PaxosPersistentData[paxocache.PartitionOperation, mapset.Set[string]]{
		Id: "id",
		Partition:   "test",
		Leader: "leader",
		HighestSlot: paxocache.POOL_DELAY - 1,
		Pools:       make(map[uint64]mapset.Set[string]),
		Chosen:      make(map[uint64]paxocache.ChosenSlot[paxocache.PartitionOperation]),
		Acceptances: make(map[uint64]paxocache.Slot[paxocache.PartitionOperation]),
	}
	singleData := paxocache.SingleData(&data)

	persistor.On("HasData").Return(true, nil)
	persistor.On("Recover").Return(&data, nil)
	persistor.On("WriteSingleData", singleData).Return(nil)

	err := paxos.BootstrapCluster()

	assert.Nilf(t, err, "Err is not nil after bootstrapping partition!")

	persistor.AssertExpectations(t)

	// Use leader to check if we actually built the root node from data
  assert.Equal(t, data.Leader, paxos.GetRootLeader())
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

	assert.Nil(t, paxosA.BootstrapCluster())
	assert.Nil(t, discoveryB.ExchangeWith(discoveryA.Address, true))

	assert.Nil(t, paxosB.BootstrapFromExisting())

	partition := "test"

	assert.Nil(t, paxosA.BootstrapPartitionForCluster(partition))
	// assert.Nil(t, paxosB.BootstrapPartitionFromExisting(partition))

	partitions := paxosB.GetPartitions()
	require.NotNil(t, partitions)
	assert.ElementsMatch(t, []string{partition}, partitions.ToSlice())
}

func TestBootstrapPartitionFromExistingRecoversData(t *testing.T) {
	logger := tests.SilentLogger()

	_, discoveryA, paxosA := createTestStack(logger)
	_, discoveryB, paxosB := createTestStack(logger)

	persistor, create := tests.CreateMockPartitionPersistor()
	paxosB.CreatePersistor = create

	partition := "test"

	require.Nilf(t, paxosA.BootstrapCluster(), "paxosA failed to bootstrap cluster!")
	require.Nilf(t, paxosA.BootstrapPartitionForCluster(partition), "Failed to bootstrap partition for cluster!")

	nodeA, err := paxosA.GetPartition(partition)
	require.Nil(t, err)

	data := paxocache.PaxosPersistentData[any, any]{
		Id: discoveryB.Address,
		Partition: partition,
		Leader: "leader",
		HighestSlot: paxocache.POOL_DELAY - 1,
		Pools: nil,
		Chosen:      make(map[uint64]paxocache.ChosenSlot[any]),
		Acceptances: make(map[uint64]paxocache.Slot[any]),
	}

	// Copy by value, not by reference to avoid data race
	paxocache.CopyMap(&nodeA.Pools, &data.Pools)

	op := struct { num int }{
		num: 123,
	}
	data.Chosen[0] = paxocache.ChosenSlot[any]{
		Number: 0,
		Type: paxocache.ChosenSlotOperation,
		Operation: op,
	}

	singleData := paxocache.SingleData(&data)

	persistor.On("HasData").Return(true, nil)
	persistor.On("Recover").Return(&data, nil)
	persistor.On("WriteSingleData", singleData).Return(nil)

	require.Nilf(t, discoveryB.ExchangeWith(discoveryA.Address, true), "discoveryB failed to exchange with discoveryA!")

	require.Nilf(t, paxosB.BootstrapFromExisting(), "paxosB failed to bootstrap from existing!")

	require.Nilf(t, paxosB.BootstrapPartitionFromExisting(partition), "Failed to bootstrap partition from existing!")
	
	// Set ProcessOperation after we've made the partition
	node, err := paxosB.GetPartition(partition)
	require.Nil(t, err)

	node.ProcessOperation = func(op any, prev any) any {
		return op.(struct { num int }).num
	}

	assert.Equal(t, op.num, paxosB.GetPartitionState(partition))
}

func TestBootstrapPartitionForClusterPullsPartitionOnOtherNodes(t *testing.T) {
	logger := tests.SilentLogger()

	_, discoveryA, paxosA := createTestStack(logger)
	_, discoveryB, paxosB := createTestStack(logger)

	require.Nil(t, discoveryB.ExchangeWith(discoveryA.Address, true))
	require.Nil(t, paxosA.BootstrapCluster())
	require.Nil(t, paxosB.BootstrapFromExisting())

	key := "key"

	require.Nil(t, paxosA.BootstrapPartitionForCluster(key))

	partition, err := paxosA.GetPartition(key)
	require.Nil(t, err)
	require.NotNil(t, partition)

	partition, err = paxosB.GetPartition(key)
	require.Nil(t, err)
	require.NotNil(t, partition)
}