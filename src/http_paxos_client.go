package paxocache

import (
	"log/slog"
)

// Must export fields for JSON to serialize them!

type WithFrom[T any] struct {
	From string
	Val T
}

type HttpPaxosClient[TOp any, TRes any] struct {
	client IHttpClient
	logger *slog.Logger

	node *PaxosNode[TOp, TRes]
	// Defaults to "" (no prefix)
	prefix string

	getRecipients func() []string
}

type PromisePayload[T any] struct {
	from string
	ballot Ballot
	success bool
	acceptances []Slot[T]
}

type AcceptPayload[T any] struct {
	from string
	ballot Ballot
	success bool
	slotIndex uint64
}

func (this *HttpPaxosClient[TOp, TRes]) forEachRecipient(f func(string)) {
	for _, host := range this.getRecipients() {
		go f(host)
	}
}

func (this *HttpPaxosClient[TOp, TRes]) withFrom(payload any) WithFrom[any] {
	return WithFrom[any] {
		Val: payload,
		From: this.node.Id,
	}
}

func (this *HttpPaxosClient[TOp, TRes]) Prepare(ballot Ballot) {
	send := func(host string) {
		var res PromisePayload[TOp]
		this.client.Post(host, this.prefix + PAXOS_PREPARE_ROUTE + "/" + this.node.Partition, this.withFrom(ballot), &res)

		this.node.ReceivePromise(res.from, res.ballot, res.success, res.acceptances)
	}
	
	this.forEachRecipient(send)
}

func (this *HttpPaxosClient[TOp, TRes]) RequestAcceptance(slot Slot[TOp]) {
	send := func(host string) {
		var res AcceptPayload[TOp]
		this.client.Post(host, this.prefix + PAXOS_REQUEST_ACCEPTANCE_ROUTE + "/" + this.node.Partition, this.withFrom(slot), &res)

		this.node.ReceiveAccept(res.from, res.ballot, res.success, res.slotIndex)
	}
	
	this.forEachRecipient(send)
}

func (this *HttpPaxosClient[TOp, TRes]) Commit(slot Slot[TOp]) {
	send := func(host string) {
		var res any
		this.client.Post(host, this.prefix + PAXOS_COMMIT_ROUTE + "/" + this.node.Partition, this.withFrom(slot), &res)
	}
	
	this.forEachRecipient(send)
}