package paxocache

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	mapset "github.com/deckarep/golang-set/v3"
)

const EXCHANGE_PATH = "/cache/exchange"

type ServerDiscoveryService struct {
	KnownHosts mapset.Set[string]
	Token      string
	address string
	Logger *slog.Logger
	Client IHttpClient
}

type ExchangePayload struct {
	// Using an interface causes problems here
	KnownHosts []string
	SentFrom string
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

	this.address = listener.Listener.Addr().String()
	this.KnownHosts.Add(listener.Listener.Addr().String())

	listener.RegisterRoute(EXCHANGE_PATH, this.handleExchange)
}

func (this *ServerDiscoveryService) handleExchange(ctx RequestContext[ExchangePayload, ExchangePayload]) {
	body, _ := ctx.DecodeBody()

	ctx.Respond(http.StatusOK, ExchangePayload{
		this.KnownHosts.ToSlice(),
		this.address,
	})

	this.learnNewHosts(body.KnownHosts, true, body.SentFrom)
}

func (this *ServerDiscoveryService) ExchangeWith(host string, contactNewHosts bool) error {
	if this.KnownHosts.Cardinality() == 0 {
		return errors.New("Tried to exchange before registering a listener! This is not allowed.")
	}

	if (!strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://")) {
		host = "http://" + host
	}
 
	var res ExchangePayload
	_, err := this.Client.Get(host, EXCHANGE_PATH, ExchangePayload{
		this.KnownHosts.ToSlice(),
		this.address,
	}, &res)

	this.Logger.Info("Received KnownHosts", "hosts", res.KnownHosts)

	this.learnNewHosts(res.KnownHosts, contactNewHosts, host)
	
	return err
}

func (this *ServerDiscoveryService) learnNewHosts(toLearn []string, contactNewHosts bool, receivedFrom string) {
	otherSet := mapset.NewSet(toLearn...)

	newHosts := otherSet.Difference(this.KnownHosts)
	newHosts.Remove(receivedFrom) // Don't send a request back to the server we got this request from

	this.KnownHosts = this.KnownHosts.Union(otherSet)

	if contactNewHosts {
		for host := range newHosts.Iter() {
			go this.ExchangeWith(host, contactNewHosts)
		}
	}
}