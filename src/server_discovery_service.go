package paxocache

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"

	mapset "github.com/deckarep/golang-set/v3"
)

const EXCHANGE_PATH = "/cache/exchange"

type ServerDiscoveryService struct {
	KnownHosts mapset.Set[string]
	Token      string
	address net.Addr
	Logger *slog.Logger
	Client IHttpClient
}

type ExchangePayload struct {
	// Using an interface causes problems here
	KnownHosts []string
}

func NewServerDiscoveryService(logger *slog.Logger) *ServerDiscoveryService {
	return &ServerDiscoveryService{
		KnownHosts: mapset.NewSet[string](),
		Logger: logger,
		Client: Http("", logger),
	}
}

func (this *ServerDiscoveryService) RegisterListener(listener *Listener) {
	this.Token = listener.Token
	if client, ok := this.Client.(*HttpClient); ok {
		client.token = listener.Token
	}

	this.address = listener.Listener.Addr()

	this.KnownHosts.Add(listener.Listener.Addr().String())

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

	if (!strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://")) {
		host = "http://" + host
	}
 
	var res ExchangePayload
	_, err := this.Client.Get(host, EXCHANGE_PATH, ExchangePayload{
		this.KnownHosts.ToSlice(),
	}, &res)

	this.Logger.Info("Received KnownHosts", "hosts", res.KnownHosts)
	this.KnownHosts = this.KnownHosts.Union(mapset.NewSet(res.KnownHosts...))

	return err
}