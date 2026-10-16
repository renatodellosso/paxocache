package paxocache

import (
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"slices"
)

type PaxosServer[T any] struct {
	logger *slog.Logger

	nodes map[string]*PaxosNode[T]
}

func (this *PaxosServer[T]) HandlePrepare(ctx RequestContext[WithFrom[Ballot], PromisePayload[T]]) {
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
	success, acceptances := node.ReceivePrepare(payload.From, ballot)

	ctx.Respond(http.StatusOK, PromisePayload[T]{
		from: node.Id,
		ballot: node.PromisedBallot,
		success: success,
		acceptances: slices.Collect(maps.Values(acceptances)),
	})
}

func (this *PaxosServer[T]) HandleRequestAcceptance(ctx RequestContext[WithFrom[Slot[T]], AcceptPayload[T]]) {
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

	success := node.ReceiveRequestAcceptance(payload.From, payload.Val)

	ctx.Respond(http.StatusOK, AcceptPayload[T]{
		from: node.Id,
		ballot: node.PromisedBallot,
		success: success,
		slotIndex: payload.Val.Number,
	})
}

func (this *PaxosServer[T]) HandleCommit(ctx RequestContext[WithFrom[Slot[T]], any]) {
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

func (this *PaxosServer[T]) RegisterListener(listener *Listener) {
	this.logger.Debug("Registering paxos server with listener...")

	listener.RegisterRoute(http.MethodPost, PAXOS_PREPARE_ROUTE + "/{partition}", this.HandlePrepare)
	listener.RegisterRoute(http.MethodPost, PAXOS_REQUEST_ACCEPTANCE_ROUTE + "/{partition}", this.HandleRequestAcceptance)
	listener.RegisterRoute(http.MethodPost, PAXOS_COMMIT_ROUTE + "/{partition}", this.HandleCommit)
}