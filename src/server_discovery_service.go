package paxocache

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	mapset "github.com/deckarep/golang-set/v3"
)

type ServerDiscoveryService struct {
	knownHosts mapset.Set[string]
	port       int
	token      string
	channel    chan DiscoveryEvent
}

// DiscoveryEventStatus enum
type DiscoveryEventStatus int

const (
	DiscoveryEventStatusError = iota
	DiscoveryEventStatusReceived
)

func (t DiscoveryEventStatus) String() string {
	strings := []string{"Error", "Received"}

	if t < 0 || int(t) >= len(strings) {
		return "Unknown"
	}

	return strings[t]
}

type DiscoveryEvent struct {
	Status DiscoveryEventStatus
	Err    error
	Msg    string
}

type IServerDiscoveryService interface {
	Listen() (<-chan DiscoveryEvent, error)
	RegisterWith(host string) error
}

func NewServerDiscoveryService(port int, token string) (IServerDiscoveryService, error) {
	return &ServerDiscoveryService{
		knownHosts: mapset.NewSet[string](),
		port:       port,
		token:      token,
		channel:    make(chan DiscoveryEvent, 16),
	}, nil
}

func (this *ServerDiscoveryService) register(w http.ResponseWriter, r *http.Request) {
	// Close response before blocking on channel
	w.WriteHeader(http.StatusNoContent)

	this.channel <- DiscoveryEvent{
		Status: DiscoveryEventStatusReceived,
		Err:    nil,
		Msg:    fmt.Sprintf("Received from %v\n", r.RemoteAddr),
	}
}

// Automatically creates a Goroutine to listen in. Blocks until listener is ready
func (this *ServerDiscoveryService) Listen() (<-chan DiscoveryEvent, error) {
	listener, err := net.Listen("tcp", ":"+fmt.Sprint(this.port))
	if err != nil {
		return this.channel, err
	}

	go func() {
		// Defer runs right before function closes out
		defer close(this.channel) // Close channel when done

		// Separate mux to allow multiple services
		mux := http.NewServeMux()

		mux.HandleFunc("/paxocache/register", this.register)
		err := http.Serve(listener, mux)

		if err != nil {
			this.channel <- DiscoveryEvent{
				Status: DiscoveryEventStatusError,
				Err:    err,
				Msg:    "",
			}
		}
	}()

	return this.channel, nil
}

func (this *ServerDiscoveryService) RegisterWith(host string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // Release when done

	url := host + "/paxocache/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", "Go-HTTP-Client/1.1")
	req.Header.Set("Accept", "text/plain")

	// Use the same port we listen on
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Execute req
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	fmt.Printf("Request status: %v\n", resp.StatusCode)

	return nil
}
