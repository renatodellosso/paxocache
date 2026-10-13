package paxocache

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"

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

type ExchangePayload struct {
	// Using an interface causes problems here
	KnownHosts []string
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
	var body ExchangePayload
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		this.logger.Error("Could not decode")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(ExchangePayload{
		this.KnownHosts.ToSlice(),
	})
	if err != nil {
		this.logger.Error("Error on handling exchange", "error", err, "route", EXCHANGE_PATH)

		return
	}

	this.KnownHosts = this.KnownHosts.Union(mapset.NewSet(body.KnownHosts...))

	w.Header().Set("Content-Type", "application/json")

	this.logger.Debug("Received exchange", "remote address", r.RemoteAddr, "route", EXCHANGE_PATH)
}

func (this *ServerDiscoveryService) ExchangeWith(host string) error {
	if this.KnownHosts.Cardinality() == 0 {
		return errors.New("Tried to exchange before registering a listener! This is not allowed.")
	}

	res, err := Http(this.token, this.logger).Get[ExchangePayload, ExchangePayload](host, EXCHANGE_PATH, ExchangePayload{
		this.KnownHosts.ToSlice(),
	})

	this.logger.Info("Received KnownHosts", "hosts", res.KnownHosts)
	this.KnownHosts = this.KnownHosts.Union(mapset.NewSet(res.KnownHosts...))

	return err
}