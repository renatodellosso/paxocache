package paxocache

type PaxosSingleData struct {
	Id string
	Partition string
	Leader string
	HighestSlot uint64
	PromisedBallot Ballot
}

func SingleData[TOp comparable, TRes any](persistent *PaxosPersistentData[TOp, TRes]) *PaxosSingleData {
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
	AppendAcceptance(slot *Slot[TOp]) error
	AppendChosen(slot *ChosenSlot[TOp]) error
	HasData() (bool, error)
	Recover() (*PaxosPersistentData[TOp, TRes], error)
}

type PaxosPersistentData[TOp comparable, TRes any] struct {
	PaxosCoreData[TOp, TRes]

	Id string
	
	// What we've accepted, maps slot index -> highest balloted value
	Acceptances map[uint64]Slot[TOp]
}

func MergeRecoveredAndPullData[TOp comparable, TRes any](id string, recovered *PaxosPersistentData[TOp, TRes], pulled *PaxosCoreData[TOp, TRes]) *PaxosPersistentData[TOp, TRes] {
	if recovered == nil {
		return &PaxosPersistentData[TOp, TRes]{
			*pulled,
			id,
			make(map[uint64]Slot[TOp]),
		}
	}

	if pulled.HighestSlot > recovered.HighestSlot {
		recovered.HighestSlot = pulled.HighestSlot
	}

	recovered.Leader = pulled.Leader

	return recovered
}