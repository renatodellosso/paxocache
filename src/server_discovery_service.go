package paxocache

import (
	"errors"
	"log/slog"
	"net"
	"net/http"

	mapset "github.com/deckarep/golang-set/v3"
)

const EXCHANGE_PATH = "/cache/exchange"

type ServerDiscoveryService struct {
	KnownHosts mapset.Set[string]
	Token      string
	address net.Addr
	logger *slog.Logger
}

type ExchangePayload struct {
	// Using an interface causes problems here
	KnownHosts []string
}

func NewServerDiscoveryService(logger *slog.Logger) *ServerDiscoveryService {
	return &ServerDiscoveryService{
		KnownHosts: mapset.NewSet[string](),
		logger: logger,
	}
}

func (this *ServerDiscoveryService) RegisterListener(listener *Listener) {
	this.Token = listener.Token
	this.address = listener.listener.Addr()

	this.KnownHosts.Add(listener.listener.Addr().String())

	listener.RegisterRoute(EXCHANGE_PATH, this.handleExchange)
}

func (this *ServerDiscoveryService) handleExchange(ctx RequestContext[ExchangePayload, ExchangePayload]) {
	body, _ := ctx.DecodeBody()

	ctx.Respond(http.StatusOK, ExchangePayload{
		this.KnownHosts.ToSlice(),
	})

	this.KnownHosts = this.KnownHosts.Union(mapset.NewSet(body.KnownHosts...))
}

func (this *ServerDiscoveryService) ExchangeWith(host string) error {
	if this.KnownHosts.Cardinality() == 0 {
		return errors.New("Tried to exchange before registering a listener! This is not allowed.")
	}
 
	var res ExchangePayload
	_, err := Http(this.Token, this.logger).Get(host, EXCHANGE_PATH, ExchangePayload{
		this.KnownHosts.ToSlice(),
	}, &res)

	this.logger.Info("Received KnownHosts", "hosts", res.KnownHosts)
	this.KnownHosts = this.KnownHosts.Union(mapset.NewSet(res.KnownHosts...))

	return err
}