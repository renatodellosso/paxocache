package paxocache

import (
	"log/slog"
	"sync"

	mapset "github.com/deckarep/golang-set/v3"
)

const ROOT_SERVER_PREFIX = "/root"

type PaxosPartition[TOp comparable, TRes any] struct {
	node *PaxosNode[TOp, TRes]
	client PaxosClient[TOp]
}

type PartitionOperationType int
const (
	PartitionCreate = iota
)

type PartitionOperation struct {
	op PartitionOperationType
	partition string
}

func processPartitionOperation(op PartitionOperation, prev mapset.Set[string]) mapset.Set[string] {
	if prev == nil {
		prev = mapset.NewSet[string]()
	}

	switch (op.op) {
	case PartitionCreate: {
		prev.Add(op.partition)
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

	logger *slog.Logger
}

func NewPaxos[TOp comparable, TRes any](logger *slog.Logger, discovery *ServerDiscoveryService) *Paxos[TOp, TRes] {
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
		logger: logger,
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
func (this *Paxos[TOp, TRes]) Bootstrap() {
	if this.discovery.KnownHosts.Difference(mapset.NewSet(this.discovery.address)).IsEmpty() {
		this.BootstrapCluster()
		return
	}
	
	this.BootstrapFromExisting()
}

// Creates and sets rootNode
func (this *Paxos[TOp, TRes]) createRootNode() {
	rootClient := HttpPaxosClient[PartitionOperation, mapset.Set[string]]{
		client: Http(this.token, this.logger),
		logger: this.logger,
		node: nil, // Make sure to set node later!
		prefix: this.rootServer.prefix,
		getRecipients: func() []string {
			return this.rootNode.GetPoolForSlot(this.rootNode.HighestSlot).ToSlice()
		},
	}

	this.rootNode = NewPaxosNode[PartitionOperation, mapset.Set[string]](this.discovery.address, "", &rootClient, this.logger, mapset.NewSet(this.discovery.address))
	this.rootNode.ProcessOperation = processPartitionOperation

	rootClient.node = this.rootNode

	this.rootServer.nodes[this.rootNode.Id] = this.rootNode
}

// Call when we're the first server to start, e.g. we haven't discovered anyone else
func (this *Paxos[TOp, TRes]) BootstrapCluster() {
	this.createRootNode()
}

// Pulls and creates one partition
func (this *Paxos[TOp, TRes]) BootstrapPartition(partition string) error {
	var data PaxosCoreData[TOp, TRes]
	_, err := this.client.Get(this.rootNode.Leader, ROOT_SERVER_PREFIX + PAXOS_PULL_ROUTE, nil, &data)

	if err != nil {
		return err
	}

	err = this.rootNode.client.Join(this.discovery.address)
	if err != nil {
		return err
	}

	partition = data.Partition

	client := HttpPaxosClient[TOp, TRes]{
		client: Http(this.token, this.logger),
		logger: this.logger,
		node: nil, // Make sure to set node later!
		getRecipients: func() []string {
			return this.partitions[partition].node.GetPoolForSlot(this.rootNode.HighestSlot).ToSlice()
		},
	}
	node := NewPaxosNode[TOp, TRes](this.discovery.address, partition, &client, this.logger, data.GetPoolForSlot(data.HighestSlot))
	client.node = node

	this.partitions[partition] = PaxosPartition[TOp, TRes]{
		node, &client,
	}
	this.server.nodes[partition] = node

	return nil
}

// Call if another server has already started
func (this *Paxos[TOp, TRes]) BootstrapFromExisting() {

}