package paxocache

import (
	"log/slog"
	"sync"

	mapset "github.com/deckarep/golang-set/v3"
)

const ROOT_SERVER_PREFIX = "/root"

type PaxosPartition[T any] struct {
	node PaxosNode[T]
	client PaxosClient[T]
}

type PartitionOperationType int
const (
	PartitionCreate = iota
)

type PartitionOperation struct {
	op PartitionOperationType
	partition string
}

type Paxos[T any] struct {
	partitions map[string]PaxosPartition[T]
	partitionMutex sync.RWMutex

	rootNode *PaxosNode[PartitionOperation]
	rootServer PaxosServer[PartitionOperation]

	server PaxosServer[T]
	discovery *ServerDiscoveryService
	token string

	logger *slog.Logger
}

func NewPaxos[T any](logger *slog.Logger, discovery *ServerDiscoveryService) *Paxos[T] {
	paxos := Paxos[T]{
		partitions: make(map[string]PaxosPartition[T]),
		rootServer: PaxosServer[PartitionOperation]{
			logger: logger,
			nodes: make(map[string]*PaxosNode[PartitionOperation]),
			prefix: ROOT_SERVER_PREFIX,
		},
		server: PaxosServer[T]{
			logger: logger,
			nodes: make(map[string]*PaxosNode[T]),
		},
		discovery: discovery,
		logger: logger,
	}

	return &paxos
}

func (this *Paxos[T]) RegisterListener(listener *Listener) {
	this.logger.Debug("Registering paxos with listener...")

	this.token = listener.Token
	this.server.RegisterListener(listener)
	this.rootServer.RegisterListener(listener)
}

// Make this Paxos[T] ready to function
func (this *Paxos[T]) Bootstrap() {
	if this.discovery.KnownHosts.Difference(mapset.NewSet(this.discovery.address)).IsEmpty() {
		this.BootstrapCluster()
		return
	}
	
	this.BootstrapFromExisting()
}

// Creates and sets rootNode
func (this *Paxos[T]) createRootNode() {
	rootClient := HttpPaxosClient[PartitionOperation]{
		client: Http(this.token, this.logger),
		logger: this.logger,
		node: nil, // Make sure to set node later!
		prefix: this.rootServer.prefix,
		getRecipients: func() []string {
			return this.rootNode.GetPoolForSlot(this.rootNode.HighestSlot).ToSlice()
		},
	}

	this.rootNode = NewPaxosNode(this.discovery.address, "", &rootClient, this.logger, mapset.NewSet(this.discovery.address))
	rootClient.node = this.rootNode

	this.rootServer.nodes[this.rootNode.Id] = this.rootNode
}

// Call when we're the first server to start, e.g. we haven't discovered anyone else
func (this *Paxos[T]) BootstrapCluster() {
	this.createRootNode()
}

// Call if another server has already started
func (this *Paxos[T]) BootstrapFromExisting() {

}