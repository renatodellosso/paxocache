package paxocache

import "maps"

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

// ~ means 'any type extending this'
func CopyMap[TMap ~map[TKey]TVal, TKey comparable, TVal any](from *TMap, to *TMap) {
	*to = make(TMap, len(*from))
	maps.Copy((*to), *from)
}

func MergeRecoveredAndPullData[TOp comparable, TRes any](id string, recovered *PaxosPersistentData[TOp, TRes], pulled *PaxosCoreData[TOp, TRes]) *PaxosPersistentData[TOp, TRes] {
	if recovered == nil {
		return &PaxosPersistentData[TOp, TRes]{
			*pulled,
			id,
			make(map[uint64]Slot[TOp]),
		}
	}

	res := *recovered

	CopyMap(&recovered.Acceptances, &res.Acceptances)
	CopyMap(&recovered.Chosen, &res.Chosen)
	CopyMap(&recovered.Pools, &res.Pools)
	
	if pulled.HighestSlot > recovered.HighestSlot {
		res.HighestSlot = pulled.HighestSlot
	}

	res.Leader = pulled.Leader

	return &res
}
