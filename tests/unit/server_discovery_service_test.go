package unit

import (
	paxocache "paxocache/src"
	"paxocache/tests"
	"strconv"
	"strings"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v3"
	"github.com/stretchr/testify/assert"
)

func TestRegisterListenerPullsAddressFromListener(t *testing.T) {
	listener := paxocache.Listener{
		Logger: tests.SilentLogger(),
	}

	discovery := paxocache.NewServerDiscoveryService(tests.SilentLogger())

	listener.Listen()
	discovery.RegisterListener(&listener)

	assert.Equal(t, 1, discovery.GetKnownHosts().Cardinality())

	address:= discovery.GetKnownHosts().ToSlice()[0]
	split := strings.Split(address, ":") // Might be in the form [::]:Port
	port, err := strconv.ParseUint(split[len(split) - 1], 10, 0)
	
	assert.Nil(t, err)
	assert.Equal(t, listener.Port, port)
}

func TestRegisterListenerExchangePullsTokenFromListener(t *testing.T) {
	listener := paxocache.Listener{
		Logger: tests.SilentLogger(),
		Token: "abc",
	}

	discovery := paxocache.NewServerDiscoveryService(tests.SilentLogger())

	listener.Listen()
	discovery.RegisterListener(&listener)
	
	assert.Equal(t, listener.Token, discovery.Token)
}

func TestExchangeWithUnionsResponseHosts(t *testing.T) {
	otherHosts := []string{"ABC", "DEF"}
	url := "http://127.0.0.1"

	client := tests.MockHttpClient{
		MockGet: func(actualUrl string, path string, payload any, result any) (int, error) {
			assert.Equal(t, url, actualUrl)
			assert.Equal(t, paxocache.EXCHANGE_PATH, path)

			result.(*paxocache.ExchangePayload).KnownHosts = otherHosts
			return 200, nil
		},
	}

	discovery :=  paxocache.ServerDiscoveryService{
		Logger: tests.SilentLogger(),
		Client: &client,
	}
	discovery.SetKnownHosts(mapset.NewSet("HIJ"))

	err := discovery.ExchangeWith(url, false)

	assert.Nil(t, err)
	assert.ElementsMatch(t, append(otherHosts, "HIJ"), discovery.GetKnownHosts().ToSlice())
}

func TestExchangeUnionsGetKnownHosts(t *testing.T) {
	listener := paxocache.Listener{
		Logger: tests.SilentLogger(),
		Token: "",
	}

	err := listener.Listen()
	assert.Nil(t, err)

	discoveryA := paxocache.NewServerDiscoveryService(tests.SilentLogger())
	discoveryA.RegisterListener(&listener)
	discoveryA.GetKnownHosts().Add("ABC")

	discoveryB := paxocache.NewServerDiscoveryService(tests.SilentLogger())
	discoveryB.Token = ""
	discoveryB.SetKnownHosts(mapset.NewSet("DEF"))

	err = discoveryB.ExchangeWith(listener.Listener.Addr().String(), false)
	assert.Nil(t, err)

	assert.ElementsMatch(t, discoveryA.GetKnownHosts().ToSlice(), discoveryB.GetKnownHosts().ToSlice())
}

func TestExchangeContactsNewHosts(t *testing.T) {
	logger := tests.SilentLogger()

	listenerA := paxocache.Listener{
		Logger: logger,
		Token: "",
	}
	listenerA.Listen()

	listenerB := paxocache.Listener{
		Logger: logger,
		Token: "",
	}
	listenerB.Listen()

	listenerC := paxocache.Listener{
		Logger: logger,
		Token: "",
	}
	listenerC.Listen()

	discoveryA := paxocache.NewServerDiscoveryService(logger)
	discoveryA.RegisterListener(&listenerA)

	discoveryB := paxocache.NewServerDiscoveryService(logger)
	discoveryB.RegisterListener(&listenerB)

	discoveryC := paxocache.NewServerDiscoveryService(logger)
	discoveryC.RegisterListener(&listenerC)

	assert.Nil(t, discoveryB.ExchangeWith(listenerA.Listener.Addr().String(), true))
	assert.Nil(t, discoveryC.ExchangeWith(listenerA.Listener.Addr().String(), true))

	// Wait some time for background requests to finish
	time.Sleep(time.Second)

	idealSet := []string {
		listenerA.Listener.Addr().String(), 
		listenerB.Listener.Addr().String(), 
		listenerC.Listener.Addr().String(),
	}

	assert.ElementsMatch(t, idealSet, discoveryA.GetKnownHosts().ToSlice())
	assert.ElementsMatch(t, idealSet, discoveryB.GetKnownHosts().ToSlice())
	assert.ElementsMatch(t, idealSet, discoveryC.GetKnownHosts().ToSlice())
}