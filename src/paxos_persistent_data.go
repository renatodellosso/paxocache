package paxocache

type PaxosSingleData struct {
	Id string
	Partition string
	Leader string
	HighestSlot uint64
	PromisedBallot Ballot
}

func PaxosSingleDataFromPersistentData[TOp comparable, TRes any](persistent *PaxosPersistentData[TOp, TRes]) *PaxosSingleData {
	return &PaxosSingleData{
		Id: persistent.Id,
		Partition: persistent.Partition,
		Leader: persistent.Leader,
		HighestSlot: persistent.HighestSlot,
		PromisedBallot: persistent.PromisedBallot,
	}
}

// Lock beforehand
type PaxosPersistor[TOp comparable, TRes any] interface {
	WriteSingleData(data *PaxosSingleData) error
	AppendPoolOp(op *PoolChangeOp) error
	AppendAcceptance(slot *Slot[TOp]) error
	AppendChosen(slot *ChosenSlot[TOp]) error
	Recover() *PaxosPersistentData[TOp, TRes]
}

type PaxosPersistentData[TOp comparable, TRes any] struct {
	PaxosCoreData[TOp, TRes]

	Id string
	
	// What we've accepted, maps slot index -> highest balloted value
	Acceptances map[uint64]Slot[TOp]
}