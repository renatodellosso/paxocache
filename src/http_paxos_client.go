package paxocache

import (
	"log/slog"
)

type HttpPaxosClient[T any] struct {
	client IHttpClient
	logger *slog.Logger

	getRecipients func() []string
	node *PaxosNode[T]
}

type PromisePayload[T any] struct {
	from string
	ballot Ballot
	success bool
	acceptances []Slot[T]
}

type AcceptPaylost[T any] struct {
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

func (this *HttpPaxosClient[T]) Prepare(ballot Ballot) {
	send := func(host string) {
		var res PromisePayload[T]
		this.client.Post(host, PAXOS_PREPARE_ROUTE, ballot, &res)

		this.node.ReceivePromise(res.from, res.ballot, res.success, res.acceptances)
	}
	
	this.forEachRecipient(send)
}

func (this *HttpPaxosClient[T]) RequestAcceptance(slot Slot[T]) {
	send := func(host string) {
		var res AcceptPaylost[T]
		this.client.Post(host, PAXOS_COMMIT_ROUTE, slot, &res)

		this.node.ReceiveAccept(res.from, res.ballot, res.success, res.slotIndex)
	}
	
	this.forEachRecipient(send)
}

func (this *HttpPaxosClient[T]) Commit(slot Slot[T]) {
	send := func(host string) {
		var res any
		this.client.Post(host, PAXOS_COMMIT_ROUTE, slot, &res)
	}
	
	this.forEachRecipient(send)
}