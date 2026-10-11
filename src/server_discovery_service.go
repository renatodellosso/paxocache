package paxocache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	mapset "github.com/deckarep/golang-set/v3"
)

type ServerDiscoveryService struct {
	KnownHosts mapset.Set[string]
	port       int
	token      string
	channel    chan DiscoveryEvent
	// DO NOT ACCESS BEFORE CALLING .Listen()!
	Listener net.Listener
}

// DiscoveryEventStatus enum
type DiscoveryEventStatus int

const (
	DiscoveryEventStatusError = iota
	DiscoveryEventStatusReceived
)

type RegisterRequestBody struct {
		Address string
}

type RegisterResponseBody struct {
	Addresses mapset.Set[string]
}

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

func NewServerDiscoveryService(port int, token string) (*ServerDiscoveryService, error) {
	return &ServerDiscoveryService{
		KnownHosts: mapset.NewSet[string](),
		port:       port,
		token:      token,
		channel:    make(chan DiscoveryEvent, 16),
	}, nil
}

func (this *ServerDiscoveryService) register(w http.ResponseWriter, r *http.Request) {
	// Decode JSON
	var body RegisterRequestBody
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	this.KnownHosts.Add(body.Address)

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(this.KnownHosts)
	if err != nil {
		this.channel <- DiscoveryEvent{
			Status: DiscoveryEventStatusError,
			Err:    err,
		}

		return
	}

	this.channel <- DiscoveryEvent{
		Status: DiscoveryEventStatusReceived,
		Err:    nil,
		Msg:    fmt.Sprintf("Received from %v\n", r.RemoteAddr),
	}
}

// Automatically creates a Goroutine to listen in. Blocks until listener is ready
func (this *ServerDiscoveryService) Listen() (<-chan DiscoveryEvent, error) {
	listener, err := net.Listen("tcp", ":"+fmt.Sprint(this.port))
	this.Listener = listener
	if err != nil {
		return this.channel, err
	}

	this.KnownHosts.Add(listener.Addr().String())

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
	if this.Listener == nil {
		return errors.New("Tried to register before listening! This is not allowed.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // Release when done

	payload := RegisterRequestBody{
		Address: this.Listener.Addr().String(),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := host + "/paxocache/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", "Go-HTTP-Client/1.1")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

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