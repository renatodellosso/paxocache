package paxocache

import (
	"encoding/json"
	"log/slog"
	"os"

	mapset "github.com/deckarep/golang-set/v3"
)

type Cache struct {
	paxos *Paxos[CacheOp, string]
	logger *slog.Logger
	createRootPersistor func() PaxosPersistor[PartitionOperation, mapset.Set[string]]
	createPersistor func(partition string) PaxosPersistor[CacheOp, string]
	
	ready bool

	preregisteredHosts mapset.Set[string]
	basePath string
	token string
}

// Creates a new cache with no token and a file persistor. Call init() to start the cache
func NewCache() *Cache {
	options := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &options))

	cache := Cache{
		logger: logger,
		token: "",
		basePath: "/cache",
		preregisteredHosts: mapset.NewSet[string](),
		ready: false,
	}

	cache.createRootPersistor = func() PaxosPersistor[PartitionOperation, mapset.Set[string]] {
		return FilePersistor[PartitionOperation, mapset.Set[string]]{
			BasePath: cache.basePath,
		}
	}
	cache.createPersistor = func(partition string) PaxosPersistor[CacheOp, string] {
		return FilePersistor[CacheOp, string]{
			BasePath: cache.basePath,
		}
	}

	return &cache
}

func (this *Cache) WithToken(token string) *Cache {
	this.token = token

	return this
}

func (this *Cache) WithBasePath(path string) *Cache {
	this.basePath = path
	
	return this
}

func (this *Cache) WithCreatePersistor(root func() PaxosPersistor[PartitionOperation, mapset.Set[string]], partition func(partition string) PaxosPersistor[CacheOp, string]) *Cache {
	this.createRootPersistor = root
	this.createPersistor = partition
	
	return this
}

func (this *Cache) WithLogger(logger *slog.Logger) *Cache {
	this.logger = logger
	
	return this
}

func (this *Cache) WithKnownHost(host string) *Cache {
	this.preregisteredHosts.Add(host)
	
	return this
}

func (this *Cache) Init() error {
	this.logger.Debug("Initializing cache...")

	listener := Listener{
		Logger: this.logger,
		Token: "",
	}
	err := listener.Listen()

	if err != nil {
		return err
	}

	this.logger.Debug("Listener is up")
	
	discovery := NewServerDiscoveryService(this.logger)
	discovery.RegisterListener(&listener)

	this.logger.Debug("Registered discovery with listener")

	for host := range this.preregisteredHosts.Iter() {
		discovery.ExchangeWith(host, true)
	}
	this.logger.Debug("Exchanged with preregistered hosts")

	this.paxos = NewPaxos(this.logger, discovery, this.createRootPersistor, this.createPersistor)
	this.paxos.RegisterListener(&listener)
	this.paxos.processOp = processCacheOp

	this.logger.Debug("Registered paxos with listener")

	err = this.paxos.Bootstrap()
	if err != nil {
		return err
	}

	this.logger.Debug("Bootstrapped Paxos")

	this.logger.Info("Cache is ready!")
	this.ready = true
	return nil
}

func (this *Cache) IsReady() bool {
	return this.ready
}

func (this *Cache) Get[T any](key string) (*T, error) {
	partition, err := this.paxos.GetPartition(key)

	if err != nil {
		// We don't have the partition locally in memory, try to pull or recover it
		err := this.paxos.BootstrapPartition(key)
		if err != nil {
			return nil, err
		}

		partition, err = this.paxos.GetPartition(key)
		if err != nil {
			return nil, err
		}
	}

	raw := partition.GetState()

	if raw == "" {
		return nil, nil
	}

	var state T
	err = json.Unmarshal([]byte(raw), &state)

	if err != nil {
		return nil, err
	}

	return &state, nil
}

func (this *Cache) Set[T any](key string, val *T) error {
	partition, err := this.paxos.GetPartition(key)
	
	if err != nil {
		// Partition is new, unrecovered, or non-local
		err := this.paxos.BootstrapPartition(key)
		if err != nil {
			return err
		}

		partition, err = this.paxos.GetPartition(key)
		if err != nil {
			return err
		}
	}

	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	_, err = partition.Propose(CacheOp{
		Op: CacheOpSet,
		Data: string(bytes),
	})

	return err
}

func (this *Cache) GetAddress() string {
	return this.paxos.discovery.Address
}

func (this *Cache) SyncWith(host string) error {
	return this.paxos.discovery.ExchangeWith(host, true)
}