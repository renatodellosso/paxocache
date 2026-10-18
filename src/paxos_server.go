package paxocache

import (
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"slices"
)

type PaxosServer[TOp comparable, TRes any] struct {
	logger *slog.Logger

	nodes map[string]*PaxosNode[TOp, TRes]
	// Defaults to "" (no prefix)
	prefix string
}

func (this *PaxosServer[TOp, TRes]) HandlePrepare(ctx RequestContext[WithFrom[Ballot], PromisePayload[TOp]]) {
	node, ok := this.nodes[ctx.Req.PathValue("partition")]

	if !ok {
		this.logger.Error(PAXOS_PREPARE_ROUTE + "/" + ctx.Req.PathValue("partition") + ": invalid partition")
		ctx.RespondWithError(http.StatusNotFound,  errors.New("Invalid partition"))
		return
	}

	payload, err := ctx.DecodeBody()
	if err != nil {
		this.logger.Error(PAXOS_PREPARE_ROUTE + "/" + ctx.Req.PathValue("partition") + ": error decoding body", "error", err)
		ctx.RespondWithError(http.StatusInternalServerError,  err)
		return
	}

	ballot := payload.Val
	success, acceptances, promisedBallot := node.ReceivePrepare(payload.From, ballot)

	ctx.Respond(http.StatusOK, PromisePayload[TOp]{
		from: node.Id,
		ballot: promisedBallot,
		success: success,
		acceptances: slices.Collect(maps.Values(acceptances)),
	})
}

func (this *PaxosServer[TOp, TRes]) HandleRequestAcceptance(ctx RequestContext[WithFrom[Slot[TOp]], AcceptPayload[TOp]]) {
	node, ok := this.nodes[ctx.Req.PathValue("partition")]

	if !ok {
		this.logger.Error(PAXOS_REQUEST_ACCEPTANCE_ROUTE + "/" + ctx.Req.PathValue("partition") + ": invalid partition")
		ctx.RespondWithError(http.StatusNotFound,  errors.New("Invalid partition"))
		return
	}

	payload, err := ctx.DecodeBody()
	if err != nil {
		this.logger.Error(PAXOS_REQUEST_ACCEPTANCE_ROUTE + "/" + ctx.Req.PathValue("partition") + ": error decoding body", "error", err)
		ctx.RespondWithError(http.StatusInternalServerError,  err)
		return
	}

	success, promisedBallot := node.ReceiveRequestAcceptance(payload.From, payload.Val)

	ctx.Respond(http.StatusOK, AcceptPayload[TOp]{
		from: node.Id,
		ballot: promisedBallot,
		success: success,
		slotIndex: payload.Val.Number,
	})
}

func (this *PaxosServer[TOp, TRes]) HandleCommit(ctx RequestContext[WithFrom[Slot[TOp]], any]) {
	node, ok := this.nodes[ctx.Req.PathValue("partition")]

	if !ok {
		this.logger.Error(PAXOS_COMMIT_ROUTE + "/" + ctx.Req.PathValue("partition") + ": invalid partition")
		ctx.RespondWithError(http.StatusNotFound,  errors.New("Invalid partition"))
		return
	}

	payload, err := ctx.DecodeBody()
	if err != nil {
		this.logger.Error(PAXOS_COMMIT_ROUTE + "/" + ctx.Req.PathValue("partition") + ": error decoding body", "error", err)
		ctx.RespondWithError(http.StatusInternalServerError,  err)
		return
	}

	node.ReceiveCommit(payload.From, payload.Val)

	ctx.Respond(http.StatusOK, nil)
}

func (this *PaxosServer[TOp, TRes]) HandlePull(ctx RequestContext[any, PaxosCoreData[TOp, TRes]]) {
	node, ok := this.nodes[ctx.Req.PathValue("partition")]

	if !ok {
		this.logger.Error(PAXOS_PULL_ROUTE + "/" + ctx.Req.PathValue("partition") + ": invalid partition")
		ctx.RespondWithError(http.StatusNotFound,  errors.New("Invalid partition"))
		return
	}

	ctx.Respond(http.StatusOK, node.getCoreData())
}

func (this *PaxosServer[TOp, TRes]) HandleJoin(ctx RequestContext[WithFrom[any], any]) {
	node, ok := this.nodes[ctx.Req.PathValue("partition")]

	if !ok {
		this.logger.Error(PAXOS_PULL_ROUTE + "/" + ctx.Req.PathValue("partition") + ": invalid partition")
		ctx.RespondWithError(http.StatusNotFound,  errors.New("Invalid partition"))
		return
	}

	payload, err := ctx.DecodeBody()
	if err != nil {
		this.logger.Error(PAXOS_PULL_ROUTE + "/" + ctx.Req.PathValue("partition") + ": error decoding body", "error", err)
		ctx.RespondWithError(http.StatusInternalServerError,  err)
		return
	}
	
	if node.Leader != node.Id {
		// Not the leader, forward request
		err := node.client.Join(payload.From)

		if err != nil {
			ctx.RespondWithError(http.StatusInternalServerError, err)
			return
		}
	}

	// Node is leader, propose pool change
	slot, err := node.ProposePoolChange(PoolChangeAdd, payload.From)

	if err != nil {
		ctx.RespondWithError(http.StatusInternalServerError, err)
		return
	}
	
	// Propose NoOps to fill POOL_DELAY
	for node.HighestSlot < slot + POOL_DELAY {
		slot, err = node.ProposeNoOp()

		if err != nil {
			ctx.RespondWithError(http.StatusInternalServerError, err)
			return
		}
	}

	ctx.Respond(http.StatusOK, nil)
}

func (this *PaxosServer[TOp, TRes]) RegisterListener(listener *Listener) {
	this.logger.Debug("Registering paxos server with listener...")

	listener.RegisterRoute(http.MethodPost, this.prefix + PAXOS_PREPARE_ROUTE + "/{partition}", this.HandlePrepare)
	listener.RegisterRoute(http.MethodPost, this.prefix + PAXOS_REQUEST_ACCEPTANCE_ROUTE + "/{partition}", this.HandleRequestAcceptance)
	listener.RegisterRoute(http.MethodPost, this.prefix + PAXOS_COMMIT_ROUTE + "/{partition}", this.HandleCommit)
	listener.RegisterRoute(http.MethodGet, this.prefix + PAXOS_PULL_ROUTE + "/{partition}", this.HandlePull)
	listener.RegisterRoute(http.MethodPost, this.prefix + PAXOS_JOIN_ROUTE + "/{partition}", this.HandleJoin)
}