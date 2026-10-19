package integration

import (
	"log/slog"
	"os"
	paxocache "paxocache/src"
	"paxocache/tests"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBootstrapClusterCreatesClusterThatCanCreatePartitions(t *testing.T) {
	// Don't use synctest unless we mock networking

	// logger := tests.SilentLogger()
	options := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &options))
	listener := paxocache.Listener{
		Logger: logger,
		Token: "",
	}
	listener.Listen()
	
	discovery := paxocache.NewServerDiscoveryService(tests.SilentLogger())
	discovery.RegisterListener(&listener)

	paxos := paxocache.NewPaxos[any, any](logger, discovery)
	paxos.RegisterListener(&listener)

	partition := "test"

	err := paxos.BootstrapCluster()
	assert.Nilf(t, err, "Err is not nil after bootstrapping cluster!")

	err = paxos.BootstrapPartitionForCluster(partition)

	assert.Nilf(t, err, "Err is not nil after bootstrapping partition!")

	partitions := paxos.GetPartitions()
	assert.NotNilf(t, partitions, "Partitions is nil!")
	assert.ElementsMatch(t, []string{partition}, partitions.ToSlice())
}