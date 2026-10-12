package tests

import (
	paxocache "paxocache/src"
	"testing"
)

func createService(t *testing.T, port int) *paxocache.ServerDiscoveryService {
	service, err := paxocache.NewServerDiscoveryService(port, "")
	if err != nil {
		t.Fatalf("Error creating service: %v\n", err)
	}

	return service
}

func TestReceivesRegistration(t *testing.T) {
	listener := createService(t, 8000)
	sender := createService(t, 8001)

	listenChan, err := listener.Listen()
	if err != nil {
		t.Fatalf("Error when trying to listen: '%v'", err)
	}

	sender.Listen()

	err = sender.ExchangeWith("http://127.0.0.1:8000")
	if err != nil {
		t.Fatalf("Error when trying to register: '%v'", err)
	}

	res := <-listenChan
	if res.Status != paxocache.DiscoveryEventStatusReceived {
		t.Fatalf("Received '%v' instead of '%v', error: '%v', message: '%v'",
			res.Status, paxocache.DiscoveryEventStatusReceived, res.Err, res.Msg)
	}

	if listener.KnownHosts.Cardinality() != 2 {
		t.Errorf("Listener has wrong number of KnownHosts: %v != %v", listener.KnownHosts.Cardinality(), 2)
	}
	if !listener.KnownHosts.Contains(sender.Listener.Addr().String()) {
		t.Errorf("Listener's KnownHosts does not contain the address of the sender. KnownHosts: %v", listener.KnownHosts.String())
	}
}