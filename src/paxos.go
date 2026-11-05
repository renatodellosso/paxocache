package paxocache

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v3"
)

const ROOT_SERVER_PREFIX = "/root"
const ROOT_PARTITION = "root"

type PaxosPartition[TOp comparable, TRes any] struct {
	node *PaxosNode[TOp, TRes]
	client PaxosClient[TOp]
}

type PartitionOperationType int
const (
	PartitionCreate = iota
)

type PartitionOperation struct {
	Op PartitionOperationType
	Partition string
}

func processPartitionOperation(op PartitionOperation, prev mapset.Set[string]) mapset.Set[string] {
	if prev == nil {
		prev = mapset.NewSet[string]()
	}

	switch (op.Op) {
	case PartitionCreate: {
		prev.Add(op.Partition)
		return prev
	}
	}

	return nil
}

type Paxos[TOp comparable, TRes any] struct {
	partitions map[string]PaxosPartition[TOp, TRes]
	partitionMutex sync.RWMutex

	rootNode *PaxosNode[PartitionOperation, mapset.Set[string]]
	rootServer PaxosServer[PartitionOperation, mapset.Set[string]]

	server PaxosServer[TOp, TRes]
	discovery *ServerDiscoveryService
	client HttpClient
	token string
	CreateRootPersistor func() PaxosPersistor[PartitionOperation, mapset.Set[string]]
	CreatePersistor func(partition string) PaxosPersistor[TOp, TRes]
	processOp func(op TOp, prev TRes) TRes

	logger *slog.Logger

	reservedInittingPartitions mapset.Set[string]
	reservedInittingPartitionsMutex sync.Mutex

	inittedPartitions mapset.Set[string]
	inittedPartitionsMutex sync.RWMutex
}

func NewPaxos[TOp comparable, TRes any](logger *slog.Logger, discovery *ServerDiscoveryService, createRootPersistor func() PaxosPersistor[PartitionOperation, mapset.Set[string]], createPersistor func(partition string) PaxosPersistor[TOp, TRes]) *Paxos[TOp, TRes] {
	paxos := Paxos[TOp, TRes]{
		partitions: make(map[string]PaxosPartition[TOp, TRes]),
		rootServer: PaxosServer[PartitionOperation, mapset.Set[string]]{
			logger: logger,
			nodes: make(map[string]*PaxosNode[PartitionOperation, mapset.Set[string]]),
			prefix: ROOT_SERVER_PREFIX,
		},
		server: PaxosServer[TOp, TRes]{
			logger: logger,
			nodes: make(map[string]*PaxosNode[TOp, TRes]),
		},
		discovery: discovery,
		CreateRootPersistor: createRootPersistor,
		CreatePersistor: createPersistor,
		logger: logger,
		reservedInittingPartitions: mapset.NewSet[string](),
		inittedPartitions: mapset.NewSet[string](),
	}

	return &paxos
}

func (this *Paxos[TOp, TRes]) RegisterListener(listener *Listener) {
	this.logger.Debug("Registering paxos with listener...")

	this.token = listener.Token
	this.client = *Http(this.token, this.logger)

	this.server.RegisterListener(listener)
	this.rootServer.RegisterListener(listener)
}

// Make this Paxos[T] ready to function
func (this *Paxos[TOp, TRes]) Bootstrap() error {
	if this.discovery.GetKnownHosts().Difference(mapset.NewSet(this.discovery.Address)).IsEmpty() {
		return this.BootstrapCluster()
	}
	
	return this.BootstrapFromExisting()
}

func (this *Paxos[TA, TB]) getDataIfExistsForPartition[TOp comparable, TRes any](persistor PaxosPersistor[TOp, TRes]) (*PaxosPersistentData[TOp, TRes], error) {
	var data *PaxosPersistentData[TOp, TRes]

	exists, err := persistor.HasData();
	if err != nil {
		return nil, err
	}
	if exists {
		data, err = persistor.Recover()

		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Creates and sets rootNode. Pass nil for data if this is the first node in the cluster
func (this *Paxos[TOp, TRes]) createRootNode(data *PaxosPersistentData[PartitionOperation, mapset.Set[string]], persistor *PaxosPersistor[PartitionOperation, mapset.Set[string]]) error {
	rootClient := HttpPaxosClient[PartitionOperation, mapset.Set[string]]{
		client: Http(this.token, this.logger),
		logger: this.logger,
		node: nil, // Make sure to set node later!
		prefix: this.rootServer.prefix,
		getRecipients: func(slot uint64) []string {
			this.rootNode.mutex.Lock()
			defer this.rootNode.mutex.Unlock()

			return this.rootNode.GetPoolForSlot(slot).ToSlice()
		},
		getAllRecipients: func(slot uint64) []string {
			this.rootNode.mutex.Lock()
			defer this.rootNode.mutex.Unlock()

			return this.rootNode.GetPoolForSlot(slot).Union(this.discovery.GetKnownHosts()).ToSlice()
		},
	}

	if data != nil {
		this.rootNode = NewPaxosNodeFromPersistentData(this.discovery.Address, &rootClient, this.logger, *persistor, data)
	} else {
		this.rootNode = NewPaxosNode(this.discovery.Address, "", &rootClient, this.logger, *persistor, mapset.NewSet(this.discovery.Address))
	}

	this.rootNode.ProcessOperation = processPartitionOperation
	this.rootNode.Partition = ROOT_PARTITION

	rootClient.node = this.rootNode

	this.rootServer.nodes[this.rootNode.Partition] = this.rootNode

	if data == nil {
		return this.rootNode.TryTakeLeadership(TIMEOUT)
	}

	return nil
}

func (this *Paxos[TOp, TRes]) createNodeForPartition(partition string, data *PaxosPersistentData[TOp, TRes]) (*HttpPaxosClient[TOp, TRes], *PaxosNode[TOp, TRes]) {
	client := HttpPaxosClient[TOp, TRes]{
		client: Http(this.token, this.logger),
		logger: this.logger,
		node: nil, // Make sure to set node later!
		getRecipients: func(slot uint64) []string {
			this.partitionMutex.RLock()
			node := this.partitions[partition].node
			this.partitionMutex.RUnlock()

			node.mutex.Lock()
			defer node.mutex.Unlock()

			pool := node.GetPoolForSlot(slot)

			if pool == nil {
				return nil
			}
			return pool.ToSlice()
		},
		getAllRecipients: func(slot uint64) []string {
			this.partitionMutex.RLock()
			node := this.partitions[partition].node
			this.partitionMutex.RUnlock()

			node.mutex.Lock()
			defer node.mutex.Unlock()

			return node.GetPoolForSlot(slot).Union(this.discovery.GetKnownHosts()).ToSlice()
		},
	}

	var node *PaxosNode[TOp, TRes]
	if data != nil {
		node = NewPaxosNodeFromPersistentData(this.discovery.Address, &client, this.logger, &NoOpPersistor[TOp, TRes]{}, data)
	} else {
		node = NewPaxosNode(this.discovery.Address, partition, &client, this.logger, &NoOpPersistor[TOp, TRes]{}, mapset.NewSet(this.discovery.Address))
	}

	node.ProcessOperation = this.processOp
	
	client.node = node

	this.partitionMutex.Lock()
	this.partitions[partition] = PaxosPartition[TOp, TRes]{
		node, &client,
	}
	this.server.nodes[partition] = node
	this.partitionMutex.Unlock()

	if data == nil {
		node.TryTakeLeadership(TIMEOUT)
	}

	return &client, node
}

// Call when we're the first server to start, e.g. we haven't discovered anyone else
func (this *Paxos[TOp, TRes]) BootstrapCluster() error {
	this.logger.Info("Bootstrapping cluster...")

	persistor := this.CreateRootPersistor()
	data, err := this.getDataIfExistsForPartition(persistor)
	if err != nil {
		this.logger.Error("Couldn't get recovery data (if any) for root node.", "error", err)
		return err
	}

	err = this.createRootNode(data, &persistor)
	if err != nil {
		this.logger.Error("Couldn't create root node.", "error", err)
		return err
	}

	this.logger.Info("Bootstrapped cluster!")
	return nil
}

func (this *Paxos[TOp, TRes]) pullRootData(hosts []string, index int) (*PaxosCoreData[PartitionOperation, mapset.Set[string]], error) {
	// Never pull from ourselves!
	if index >= len(hosts) {
		return nil, errors.New("All hosts failed")
	}

	host := hosts[index]
	if host == this.discovery.Address {
		return this.pullRootData(hosts, index + 1)
	}

	var data PaxosCoreData[PartitionOperation, mapset.Set[string]]
	_, err := this.client.Get(host, ROOT_SERVER_PREFIX + PAXOS_PULL_ROUTE + "/" + ROOT_PARTITION, nil, &data)

	if err != nil {
		if index < this.discovery.GetKnownHosts().Cardinality() - 1 {
			return this.pullRootData(hosts, index + 1)
		}
		return nil, err
	}

	return &data, nil
}

// Call if another server has already started. Will also bootstrap any existing partitions
func (this *Paxos[TOp, TRes]) BootstrapFromExisting() error {
	this.logger.Info("Bootstrapping from existing...")

	pulled, err := this.pullRootData(this.discovery.GetKnownHosts().ToSlice(), 0)
	if err != nil {
		return err
	}

	persistor := this.CreateRootPersistor()
	data, err := this.getDataIfExistsForPartition(persistor)

	if err != nil {
		this.logger.Error("Couldn't get recovery data (if any) for root node.", "error", err)
		return err
	}

	data = MergeRecoveredAndPullData(this.discovery.Address, data, pulled)

	err = this.createRootNode(data, &persistor)
	if err != nil {
		this.logger.Error("Couldn't bootstrap from existing.", "error", err)
		return err
	}

	// Join the cluster if we haven't already
	inPool, determinable := this.rootNode.isInPool(this.rootNode.HighestSlot, this.rootNode.Id)
	if !inPool || !determinable {
		err := this.rootNode.client.Join(this.rootNode.Id)
		if err != nil {
			this.logger.Error("Couldn't bootstrap from existing.", "error", err)
			return err
		}
	}

	partitions := this.rootNode.GetState()
	this.logger.Info("Bootstrapping individual partitions", "partitions", partitions)

	if partitions != nil {
		for partition := range partitions.Iter() {
			go this.BootstrapPartitionFromExisting(partition)
		}
	}

	this.logger.Info("Bootstrapped from existing!")
	return nil
}

func (this *Paxos[TOp, TRes]) BootstrapPartition(partition string) error {
	if this.GetPartitions().Contains(partition) {
		return this.BootstrapPartitionFromExisting(partition)
	}

	return this.BootstrapPartitionForCluster(partition)
}

// Globally creates a partition
func (this *Paxos[TOp, TRes]) BootstrapPartitionForCluster(partition string) error {
	if !this.TryReservePartitionForCreation(partition) {
		return this.BlockUntilPartitionIsInitted(partition)
	}

	this.logger.Debug("Bootstrapping partition for cluster", "partition", partition)

	persistor := this.CreatePersistor(partition)
	data, err := this.getDataIfExistsForPartition(persistor)

	if err != nil {
		this.logger.Error("Couldn't get recovery data (if any) for partition.", "partition", partition, "error", err)
		
		this.UnreservePartition(partition)
		return err
	}

	if data == nil {
		// Propose creating partition as it doesn't exist
		err := this.rootNode.client.Propose(ChosenSlotOperation, PartitionOperation{
			PartitionCreate,
			partition,
		})

		if err != nil {
			this.logger.Error("Couldn't bootstrap partition.", "error", err)

			this.UnreservePartition(partition)
			return err
		}
	}

	this.createNodeForPartition(partition, data)

	this.MarkPartitionInitted(partition)

	this.logger.Info("Bootstrapped partition!")
	return nil
}

// Pulls and locally creates one partition
func (this *Paxos[TOp, TRes]) BootstrapPartitionFromExisting(partition string) error {
	if !this.TryReservePartitionForCreation(partition) {
		return this.BlockUntilPartitionIsInitted(partition)
	}

	var pulled PaxosCoreData[TOp, TRes]

	this.rootNode.mutex.Lock()
	leader := this.rootNode.Leader
	this.rootNode.mutex.Unlock()

	_, err := this.client.Get(leader,  PAXOS_PULL_ROUTE + "/" + partition, nil, &pulled)

	if err != nil {
		this.UnreservePartition(partition)
		return err
	}

	persistor := this.CreatePersistor(partition)
	data, err := this.getDataIfExistsForPartition(persistor)

	if err != nil {
		this.logger.Error("Couldn't get recovery data (if any) for partition.", "partition", partition, "error", err)

		this.UnreservePartition(partition)
		return err
	}

	data = MergeRecoveredAndPullData(this.discovery.Address, data, &pulled)

	client, node := this.createNodeForPartition(data.Partition, data)

	err = client.Join(node.Id)
	if err != nil {
		this.UnreservePartition(partition)
		return err
	}

	this.MarkPartitionInitted(partition)
	return nil
}

func (this *Paxos[TOp, TRes]) GetPartitions() mapset.Set[string] {
	res := this.rootNode.GetState()

	// Zero partitions is nil, not an empty set
	if res == nil {
		return mapset.NewSet[string]()
	}

	return res
}

// Returns what the root node's pool will be after all committed pool changes take effect (effectively HighestSlot + POOL_DELAY)
func (this *Paxos[TOp, TRes]) GetRootPool() mapset.Set[string] {
	this.rootNode.mutex.Lock()
	defer this.rootNode.mutex.Unlock()

	return this.rootNode.GetPoolForSlot(this.rootNode.HighestSlot + POOL_DELAY)
}

func (this *Paxos[TOp, TRes]) GetRootLeader() string {
	this.rootNode.mutex.Lock()
	defer this.rootNode.mutex.Unlock()

	return this.rootNode.Leader
}

func (this *Paxos[TOp, TRes]) GetPartitionLeader(partition string) string {
	this.partitionMutex.RLock()
	node := this.partitions[partition].node
	this.partitionMutex.RUnlock()

	node.mutex.Lock()
	defer node.mutex.Unlock()

	return node.Leader
}

func (this *Paxos[TOp, TRes]) GetPartitionState(partition string) TRes {
	this.partitionMutex.RLock()
	node := this.partitions[partition].node
	this.partitionMutex.RUnlock()

	return node.GetState()
}

func (this *Paxos[TOp, TRes]) GetPartition(partition string) (*PaxosNode[TOp, TRes], error) {
	this.partitionMutex.RLock()
	paxosPartition, ok := this.partitions[partition]
	this.partitionMutex.RUnlock()

	if !ok {
		return nil, errors.New("Could not find partition: " + partition)
	}

	return paxosPartition.node, nil
}

// Returns success
func (this *Paxos[TOp, TRes]) TryReservePartitionForCreation(partition string) bool {
	this.reservedInittingPartitionsMutex.Lock()
	defer this.reservedInittingPartitionsMutex.Unlock()

	success := !this.reservedInittingPartitions.ContainsOne(partition)
	this.reservedInittingPartitions.Add(partition)
	
	return success
}

func (this *Paxos[TOp, TRes]) MarkPartitionInitted(partition string) {
	this.inittedPartitionsMutex.Lock()
	defer this.inittedPartitionsMutex.Unlock()

	this.inittedPartitions.Add(partition)
}

func (this *Paxos[TOp, TRes]) UnreservePartition(partition string) {
	this.reservedInittingPartitionsMutex.Lock()
	defer this.reservedInittingPartitionsMutex.Unlock()

	this.reservedInittingPartitions.Remove(partition)
}

func (this *Paxos[TOp, TRes]) BlockUntilPartitionIsInitted(partition string) error {
	timeout := TIMEOUT
	delay := INITIAL_POLL_DELAY

	for timeout > 0 {
		this.inittedPartitionsMutex.RLock()
		complete := this.inittedPartitions.ContainsOne(partition)
		this.inittedPartitionsMutex.RUnlock()

		if complete {
			return nil
		}

		time.Sleep(delay)
		timeout -= delay
		delay *= 2
	}

	return errors.New("Timeout expired waiting for partition to finish bootstrapping")
}