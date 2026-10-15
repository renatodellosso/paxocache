package paxocache

import (
	"log/slog"
)

type HttpPaxosClient[T any] struct {
	client IHttpClient
	logger *slog.Logger
	getRecipients func() []string
}

func (this *HttpPaxosClient[T]) sendToRecipients(route string, payload any) {
	for _, host := range this.getRecipients() {
		var res error = nil
		this.client.Post(host, route, payload, &res)

		if res != nil {
			this.logger.Error("Received error", "host", host, "route", route, "res", res)
		}
	}
}

func (this *HttpPaxosClient[T]) Prepare(ballot Ballot) {
	this.sendToRecipients(PAXOS_PREPARE_ROUTE, ballot)
}

func (this *HttpPaxosClient[T]) RequestAcceptance(slot Slot[T]) {
	this.sendToRecipients(PAXOS_REQUEST_ACCEPTANCE_ROUTE, slot)
}

func (this *HttpPaxosClient[T]) Commit(slot Slot[T]) {
	this.sendToRecipients(PAXOS_COMMIT_ROUTE, slot)
}