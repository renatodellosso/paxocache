package paxocache

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type ServerDiscoveryService struct {
	knownHosts []string
	token string
}

// DiscoveryEventStatus enum
type DiscoveryEventStatus int

const (
	DiscoveryEventStatusError = iota
	DiscoveryEventStatusListening
	DiscoveryEventStatusReceived
)

func (t DiscoveryEventStatus) String() string {
	strings := []string{ "Error", "Listening", "Received" }

	if t < 0 || int(t) >= len(strings) {
		return "Unknown"
	}

	return strings[t]
}

type DiscoveryEvent struct {
	status DiscoveryEventStatus
	err error
	msg string
}

type IServerDiscoveryService interface {
	Listen() <- chan DiscoveryEvent
	RegisterWith(host string) error
}

func NewServerDiscoveryService(token string) (IServerDiscoveryService, error) {
	return &ServerDiscoveryService{
		knownHosts: []string{},
		token: token,
	}, nil
}

func (this *ServerDiscoveryService) register(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("Received from %v\n", r.RemoteAddr)
}

// Automatically creates a Goroutine to listen in
func (this *ServerDiscoveryService) Listen() <- chan DiscoveryEvent {
	out := make(chan DiscoveryEvent)

	go func() {
		// Defer runs right before function closes out
		defer close(out) // Close channel when done

		http.HandleFunc("/paxocache/register", this.register)
		err := http.ListenAndServe(":8080", nil)

		if err != nil {
			out <- DiscoveryEvent{
				status: DiscoveryEventStatusError,
				err: err,
				msg: "",
			}
		}
	}()

	return out
}

func (this *ServerDiscoveryService) RegisterWith(host string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel() // Release when done

	url := host + "/paxocache/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", "Go-HTTP-Client/1.1")
	req.Header.Set("Accept", "text/plain")

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Execute req
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	fmt.Printf("Request status: %v\n", resp.StatusCode)

	return nil
}