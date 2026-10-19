package paxocache

import (
	"errors"
	"log/slog"
)

// Must export fields for JSON to serialize them!

type WithFrom[T any] struct {
	From string
	Val T
}

type HttpPaxosClient[TOp comparable, TRes any] struct {
	client IHttpClient
	logger *slog.Logger

	node *PaxosNode[TOp, TRes]
	// Defaults to "" (no prefix)
	prefix string

	getRecipients func() []string
}

// Export fields so JSON serializes them

type PromisePayload[T comparable] struct {
	From string
	Ballot Ballot
	Success bool
	Acceptances []Slot[T]
}

type AcceptPayload struct {
	From string
	Ballot Ballot
	Success bool
	SlotIndex uint64
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
		status, err := this.client.Post(host, this.prefix + PAXOS_PREPARE_ROUTE + "/" + this.node.Partition, this.withFrom(ballot), &res)

		this.logger.Debug("Prepare result", "host", host, "status", status, "err", err, "res", res)

		this.node.ReceivePromise(res.From, res.Ballot, res.Success, res.Acceptances)
	}
	
	this.forEachRecipient(send)
}

func (this *HttpPaxosClient[TOp, TRes]) RequestAcceptance(slot Slot[TOp]) {
	send := func(host string) {
		var res AcceptPayload
		this.client.Post(host, this.prefix + PAXOS_REQUEST_ACCEPTANCE_ROUTE + "/" + this.node.Partition, this.withFrom(slot), &res)

		this.node.ReceiveAccept(res.From, res.Ballot, res.Success, res.SlotIndex)
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

func (this *HttpPaxosClient[TOp, TRes]) Join(from string) error {
	var res bool
	_, err := this.client.Post(this.node.Leader, this.prefix + PAXOS_JOIN_ROUTE + "/" + this.node.Partition, WithFrom[any]{
		From: from,
		Val: nil,
	}, &res)

	if err != nil {
		return err
	}

	if res != false {
		return errors.New("Failed to join pool") 
	}

	return nil
}

func (this *HttpPaxosClient[TOp, TRes]) Propose(val TOp) error {
	var res bool
	_, err := this.client.Post(this.node.Leader, this.prefix + PAXOS_PROPOSE_ROUTE + "/" + this.node.Partition, val, &res)

	if err != nil {
		return err
	}

	if res != false {
		return errors.New("Failed to propose") 
	}

	return nil
}