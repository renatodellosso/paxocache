package paxocache

import (
	"errors"
	"log/slog"
	"net/http"
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
		From: node.Id,
		Ballot: promisedBallot,
		Success: success,
		Acceptances: acceptances,
	})
}

func (this *PaxosServer[TOp, TRes]) HandleRequestAcceptance(ctx RequestContext[WithFrom[Slot[TOp]], AcceptPayload]) {
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

	if payload.From == "" {
		this.logger.Debug("Server:RequestAcceptance", "id", ctx.Req.Header.Get(REQUEST_ID_HEADER), "from", payload.From)
	}
	success, promisedBallot := node.ReceiveRequestAcceptance(payload.From, payload.Val)

	ctx.Respond(http.StatusOK, AcceptPayload{
		From: node.Id,
		Ballot: promisedBallot,
		Success: success,
		SlotIndex: payload.Val.Number,
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

	data := node.getCoreData()

	this.logger.Debug("Got core data", "data", data)
	ctx.Respond(http.StatusOK, data)
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
		this.logger.Error(PAXOS_JOIN_ROUTE + "/" + ctx.Req.PathValue("partition") + ": error decoding body", "error", err)
		ctx.RespondWithError(http.StatusInternalServerError,  err)
		return
	}
	
	node.mutex.Lock()
	leader := node.Leader
	node.mutex.Unlock()

	if leader != node.Id {
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
	node.mutex.Lock()
	highestSlot := node.HighestSlot
	node.mutex.Unlock()

	for highestSlot < slot + POOL_DELAY {
		_, err = node.ProposeNoOp()

		if err != nil {
			ctx.RespondWithError(http.StatusInternalServerError, err)
			return
		}


		node.mutex.Lock()
		highestSlot = node.HighestSlot
		node.mutex.Unlock()
	}

	ctx.Respond(http.StatusOK, nil)
}

func (this *PaxosServer[TOp, TRes]) HandlePropose(ctx RequestContext[TOp, any]) {
	this.logger.Debug(PAXOS_PROPOSE_ROUTE, "partition", ctx.Req.PathValue("partition"))
	node, ok := this.nodes[ctx.Req.PathValue("partition")]

	if !ok {
		err := errors.New("Invalid partition")
		this.logger.Error(PAXOS_PROPOSE_ROUTE, "partition", ctx.Req.PathValue("partition"), "success", false, "error", err)
		ctx.RespondWithError(http.StatusNotFound,  err)
		return
	}

	payload, err := ctx.DecodeBody()
	if err != nil {
		this.logger.Error(PAXOS_PROPOSE_ROUTE, "partition", ctx.Req.PathValue("partition"), "success", false, "error", err)
		ctx.RespondWithError(http.StatusInternalServerError,  err)
		return
	}
	
	if node.Leader != node.Id {
		// Not the leader, forward request
		err := node.client.Propose(payload)

		if err != nil {
			this.logger.Error(PAXOS_PROPOSE_ROUTE, "partition", ctx.Req.PathValue("partition"), "success", false, "error", err)
			ctx.RespondWithError(http.StatusInternalServerError, err)
			return
		}
	}

	// Node is leader, propose change
	_, err = node.Propose(payload)

	if err != nil {
		this.logger.Error(PAXOS_PROPOSE_ROUTE, "partition", ctx.Req.PathValue("partition"), "success", false, "error", err)
		ctx.RespondWithError(http.StatusInternalServerError, err)
		return
	}


	this.logger.Error(PAXOS_PROPOSE_ROUTE, "partition", ctx.Req.PathValue("partition"), "success", true)
	ctx.Respond(http.StatusOK, nil)
}

func (this *PaxosServer[TOp, TRes]) RegisterListener(listener *Listener) {
	this.logger.Debug("Registering paxos server with listener...")

	listener.RegisterRoute(http.MethodPost, this.prefix + PAXOS_PREPARE_ROUTE + "/{partition}", this.HandlePrepare)
	listener.RegisterRoute(http.MethodPost, this.prefix + PAXOS_REQUEST_ACCEPTANCE_ROUTE + "/{partition}", this.HandleRequestAcceptance)
	listener.RegisterRoute(http.MethodPost, this.prefix + PAXOS_COMMIT_ROUTE + "/{partition}", this.HandleCommit)
	listener.RegisterRoute(http.MethodGet, this.prefix + PAXOS_PULL_ROUTE + "/{partition}", this.HandlePull)
	listener.RegisterRoute(http.MethodPost, this.prefix + PAXOS_JOIN_ROUTE + "/{partition}", this.HandleJoin)
	listener.RegisterRoute(http.MethodPost, this.prefix + PAXOS_PROPOSE_ROUTE + "/{partition}", this.HandlePropose)
}