package tests

import (
	paxocache "paxocache/src"
	"testing"
)

func createService(t *testing.T, port int) paxocache.IServerDiscoveryService {
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

	err = sender.RegisterWith("http://127.0.0.1:8000")
	if err != nil {
		t.Fatalf("Error when trying to register: '%v'", err)
	}

	res := <-listenChan
	if res.Status != paxocache.DiscoveryEventStatusReceived {
		t.Fatalf("Received '%v' instead of '%v', error: '%v', message: '%v'",
			res.Status, paxocache.DiscoveryEventStatusReceived, res.Err, res.Msg)
	}
}