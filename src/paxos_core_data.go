package paxocache

// Data needed to join the cluster
type PaxosCoreData[TOp comparable, TRes any] struct {
	Partition string
	Leader string

	// Maps slot number to pool
	Pools PoolMap

	// Highest ballot we've promised
	PromisedBallot Ballot

	// Highest slot index we've seen or promised
	HighestSlot uint64

	// Slots already chosen
	Chosen map[uint64]ChosenSlot[TOp]
}