package tests

import (
	paxocache "paxocache/src"
	"strconv"
	"strings"
	"testing"

	mapset "github.com/deckarep/golang-set/v3"
	"github.com/stretchr/testify/assert"
)

func TestRegisterListenerPullsAddressFromListener(t *testing.T) {
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

func TestRegisterListenerExchangePullsTokenFromListener(t *testing.T) {
	listener := paxocache.Listener{
		Logger: SilentLogger(),
		Token: "abc",
	}

	discovery := paxocache.NewServerDiscoveryService(SilentLogger())

	listener.Listen()
	discovery.RegisterListener(&listener)
	
	assert.Equal(t, listener.Token, discovery.Token)
}

func TestExchangeWithUnionsResponseHosts(t *testing.T) {
	otherHosts := []string{"ABC", "DEF"}
	url := "http://127.0.0.1"

	client := MockHttpClient{
		get: func(actualUrl string, path string, payload any, result any) (int, error) {
			assert.Equal(t, url, actualUrl)
			assert.Equal(t, paxocache.EXCHANGE_PATH, path)

			result.(*paxocache.ExchangePayload).KnownHosts = otherHosts
			return 200, nil
		},
	}

	discovery := paxocache.ServerDiscoveryService{
		KnownHosts: mapset.NewSet("HIJ"),
		Logger: SilentLogger(),
		Client: &client,
	}

	err := discovery.ExchangeWith(url)

	assert.Nil(t, err)
	assert.ElementsMatch(t, append(otherHosts, "HIJ"), discovery.KnownHosts.ToSlice())
}

func TestExchangeUnionsKnownHosts(t *testing.T) {
	listener := paxocache.Listener{
		Logger: SilentLogger(),
		Token: "",
	}

	err := listener.Listen()
	assert.Nil(t, err)

	discoveryA := paxocache.NewServerDiscoveryService(SilentLogger())
	discoveryA.RegisterListener(&listener)
	discoveryA.KnownHosts.Add("ABC")

	discoveryB := paxocache.NewServerDiscoveryService(SilentLogger())
	discoveryB.Token = ""
	discoveryB.KnownHosts.Add("DEF")

	err = discoveryB.ExchangeWith(listener.Listener.Addr().String())
	assert.Nil(t, err)

	assert.ElementsMatch(t, discoveryA.KnownHosts.ToSlice(), discoveryB.KnownHosts.ToSlice())
}