package tests

import (
	paxocache "paxocache/src"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPullsAddressFromListener(t *testing.T) {
	listener := paxocache.Listener{
		Logger: SilentLogger(),
	}

	discovery := paxocache.NewServerDiscoveryService(SilentLogger())

	listener.Listen()
	discovery.RegisterListener(&listener)

	assert.Equal(t, 1, discovery.KnownHosts.Cardinality())

	address:= discovery.KnownHosts.ToSlice()[0]
	split := strings.Split(address, ":") // Might be in the form [::]:Port
	port, err := strconv.ParseUint(split[len(split) - 1], 10, 0)
	
	assert.Nil(t, err)
	assert.Equal(t, listener.Port, port)
}

func TestPullsTokenFromListener(t *testing.T) {
	listener := paxocache.Listener{
		Logger: SilentLogger(),
		Token: "abc",
	}

	discovery := paxocache.NewServerDiscoveryService(SilentLogger())

	listener.Listen()
	discovery.RegisterListener(&listener)
	
	assert.Equal(t, listener.Token, discovery.Token)
}