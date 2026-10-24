package paxocache

import (
	"errors"
	"fmt"
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

	getRecipients func(slot uint64) []string
	// Gets everyone, including those not in the current pool
	getAllRecipients func(slot uint64) []string
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

func (this *HttpPaxosClient[TOp, TRes]) forEachRecipient(includeAll bool, slot uint64, f func(string)) {
	var recipients []string

	if includeAll {
		recipients = this.getAllRecipients(slot)
	} else {
		recipients = this.getRecipients(slot)
	}

	for _, host := range recipients {
		go f(host)
	}
}

func (this *HttpPaxosClient[TOp, TRes]) withFrom(payload any) WithFrom[any] {
	if this.node.Id == "" {
		this.logger.Error("Attempted to use withFrom without an ID!", "payload", payload, "partition", this.node.Partition)
	}

	return WithFrom[any] {
		Val: payload,
		From: this.node.Id,
	}
}

func (this *HttpPaxosClient[TOp, TRes]) Prepare(ballot Ballot) {
	send := func(host string) {
		var res PromisePayload[TOp]
		
		status, err := this.client.Post(host, this.prefix + PAXOS_PREPARE_ROUTE + "/" + this.node.Partition, this.withFrom(ballot), &res)
		if err != nil {
			this.logger.Error("Error on Prepare from client", "id", this.node.Id, "partition", this.node.Partition, "host", host, "error", err.Error())
		}
		this.logger.Debug("Prepare result", "host", host, "status", status, "err", err, "res", res)

		this.node.ReceivePromise(res.From, res.Ballot, res.Success, res.Acceptances)
	}

	this.node.mutex.Lock()
	slot := this.node.HighestSlot
	this.node.mutex.Unlock()
	
	this.forEachRecipient(false, slot, send)
}

func (this *HttpPaxosClient[TOp, TRes]) RequestAcceptance(slot Slot[TOp]) {
	send := func(host string) {
		var res AcceptPayload
		_, err := this.client.Post(host, this.prefix + PAXOS_REQUEST_ACCEPTANCE_ROUTE + "/" + this.node.Partition, this.withFrom(slot), &res)
		if err != nil {
			this.logger.Error("Error on RequestAcceptance from client", "id", this.node.Id, "partition", this.node.Partition, "host", host, "error", err.Error())
		}

		this.node.ReceiveAccept(res.From, res.Ballot, res.Success, res.SlotIndex)
	}

	this.forEachRecipient(false, slot.Number, send)
}

func (this *HttpPaxosClient[TOp, TRes]) Commit(slot Slot[TOp]) {
	send := func(host string) {
		var res any
		_, err := this.client.Post(host, this.prefix + PAXOS_COMMIT_ROUTE + "/" + this.node.Partition, this.withFrom(slot), &res)
		if err != nil {
			this.logger.Error("Error on Commit from client", "id", this.node.Id, "partition", this.node.Partition, "host", host, "error", err.Error())
		}
	}
	
	this.forEachRecipient(true, slot.Number, send)
}

func (this *HttpPaxosClient[TOp, TRes]) Join(from string) error {
	if from == "" {
		return fmt.Errorf("Attempted to join without 'from'! Partition: %v, ID: %v", this.node.Partition, this.node.Id)
	}

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