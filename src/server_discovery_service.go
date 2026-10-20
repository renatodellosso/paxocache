package paxocache

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	mapset "github.com/deckarep/golang-set/v3"
)

const EXCHANGE_PATH = "/cache/exchange"

type ServerDiscoveryService struct {
	mutex sync.RWMutex
	knownHosts mapset.Set[string]
	Token      string
	Address string
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
		knownHosts: mapset.NewSet[string](),
		Logger: logger,
		Client: Http("", logger),
	}
}

func (this *ServerDiscoveryService) RegisterListener(listener *Listener) {
	this.Logger.Debug("Registering server discovery with listener...")

	this.Token = listener.Token
	if client, ok := this.Client.(*HttpClient); ok {
		client.token = listener.Token
	}

	this.Address = listener.Listener.Addr().String()

	this.mutex.Lock()
	this.knownHosts.Add(listener.Listener.Addr().String())
	this.mutex.Unlock()

	listener.RegisterRoute(http.MethodPost, EXCHANGE_PATH, this.handleExchange)
}

func (this *ServerDiscoveryService) handleExchange(ctx RequestContext[ExchangePayload, ExchangePayload]) {
	body, _ := ctx.DecodeBody()

	this.mutex.RLock()
	hosts := this.knownHosts.ToSlice()
	this.mutex.RUnlock()

	ctx.Respond(http.StatusOK, ExchangePayload{
		hosts,
		this.Address,
	})

	this.learnNewHosts(body.KnownHosts, true, body.SentFrom)
}

func (this *ServerDiscoveryService) ExchangeWith(host string, contactNewHosts bool) error {
	this.mutex.RLock()
	hostsSize := this.knownHosts.Cardinality()
	this.mutex.RUnlock()

	if hostsSize == 0 {
		return errors.New("Tried to exchange before registering a listener! This is not allowed.")
	}

	if (!strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://")) {
		host = "http://" + host
	}
 
	this.mutex.RLock()
	hosts := this.knownHosts.ToSlice()
	this.mutex.RUnlock()

	var res ExchangePayload
	_, err := this.Client.Post(host, EXCHANGE_PATH, ExchangePayload{
		hosts,
		this.Address,
	}, &res)

	this.Logger.Info("Received knownHosts", "hosts", res.KnownHosts)

	this.learnNewHosts(res.KnownHosts, contactNewHosts, host)
	
	return err
}

func (this *ServerDiscoveryService) learnNewHosts(toLearn []string, contactNewHosts bool, receivedFrom string) {
	this.mutex.Lock()
	
	otherSet := mapset.NewSet(toLearn...)

	oldHosts := this.knownHosts
	newHosts := otherSet.Difference(oldHosts)

	newHosts.Remove(receivedFrom) // Don't send a request back to the server we got this request from

	this.knownHosts = oldHosts.Union(otherSet)

	this.mutex.Unlock()

	if contactNewHosts {
		for host := range newHosts.Iter() {
			go this.ExchangeWith(host, contactNewHosts)
		}
	}
}

func (this *ServerDiscoveryService) GetKnownHosts() mapset.Set[string] {
	this.mutex.RLock()
	defer this.mutex.RUnlock()

	return this.knownHosts.Clone()
}

func (this *ServerDiscoveryService) SetKnownHosts(hosts mapset.Set[string]) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.knownHosts = hosts
}