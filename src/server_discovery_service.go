package paxocache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	mapset "github.com/deckarep/golang-set/v3"
)

const EXCHANGE_PATH = "/cache/exchange"

type ServerDiscoveryService struct {
	KnownHosts mapset.Set[string]
	port       int
	token      string
	address net.Addr
	logger *slog.Logger
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

func NewServerDiscoveryService(logger *slog.Logger) (*ServerDiscoveryService, error) {
	return &ServerDiscoveryService{
		KnownHosts: mapset.NewSet[string](),
		logger: logger,
	}, nil
}

func (this *ServerDiscoveryService) RegisterListener(listener *Listener) {
	this.token = listener.Token
	this.address = listener.listener.Addr()

	this.KnownHosts.Add(listener.listener.Addr().String())

	listener.mux.HandleFunc(EXCHANGE_PATH, this.handleExchange)
}

func (this *ServerDiscoveryService) handleExchange(w http.ResponseWriter, r *http.Request) {
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
		this.logger.Error("Error on handling exchange", "error", err, "route", EXCHANGE_PATH)

		return
	}

	this.logger.Debug("Received exchange", "remote address", r.RemoteAddr, "route", EXCHANGE_PATH)
}

func (this *ServerDiscoveryService) ExchangeWith(host string) error {
	if this.address == nil {
		return errors.New("Tried to exchange before registering a listener! This is not allowed.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // Release when done

	payload := RegisterRequestBody{
		Address: this.address.String(),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := host + EXCHANGE_PATH
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", "Go-HTTP-Client/1.1")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Bearer-Token", this.token)

	client := &http.Client{
			Timeout: 10 * time.Second,
	}

	// Execute req
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	this.logger.Debug("Received response from exchange", "status code", resp.StatusCode)

	return nil
}