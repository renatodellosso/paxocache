package paxocache

import (
	"log/slog"
)

type WithFrom[T any] struct {
	from string
	val T
}

type HttpPaxosClient[T any] struct {
	client IHttpClient
	logger *slog.Logger

	node *PaxosNode[T]

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

func (this *HttpPaxosClient[T]) forEachRecipient(f func(string)) {
	for _, host := range this.getRecipients() {
		go f(host)
	}
}

func (this *HttpPaxosClient[T]) withFrom(payload any) WithFrom[any] {
	return WithFrom[any] {
		val: payload,
		from: this.node.Id,
	}
}

func (this *HttpPaxosClient[T]) Prepare(ballot Ballot) {
	send := func(host string) {
		var res PromisePayload[T]
		this.client.Post(host, PAXOS_PREPARE_ROUTE + "/" + this.node.Partition, this.withFrom(ballot), &res)

		this.node.ReceivePromise(res.from, res.ballot, res.success, res.acceptances)
	}
	
	this.forEachRecipient(send)
}

func (this *HttpPaxosClient[T]) RequestAcceptance(slot Slot[T]) {
	send := func(host string) {
		var res AcceptPayload[T]
		this.client.Post(host, PAXOS_COMMIT_ROUTE + "/" + this.node.Partition, this.withFrom(slot), &res)

		this.node.ReceiveAccept(res.from, res.ballot, res.success, res.slotIndex)
	}
	
	this.forEachRecipient(send)
}

func (this *HttpPaxosClient[T]) Commit(slot Slot[T]) {
	send := func(host string) {
		var res any
		this.client.Post(host, PAXOS_COMMIT_ROUTE + "/" + this.node.Partition, this.withFrom(slot), &res)
	}
	
	this.forEachRecipient(send)
}