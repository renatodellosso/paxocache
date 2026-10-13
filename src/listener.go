package paxocache

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
)

type Listener struct {
	// Set to 0 to automatically choose
	Port       uint
	Token      string
	Logger *slog.Logger

	// DO NOT ACCESS BEFORE CALLING .Listen()!
	listener net.Listener
	mux *http.ServeMux
}

type RequestContext[TReq any, TRes any] struct {
	path string
	Req *http.Request
	writer http.ResponseWriter
	logger *slog.Logger
}

// If there's an error, automatically responds to the request with 500 and the error
func (this *RequestContext[TReq, TRes]) DecodeBody() (TReq, error) {
	var body TReq
	err := json.NewDecoder(this.Req.Body).Decode(&body)

	if err != nil {
		this.RespondWithError(http.StatusInternalServerError, err)
	}

	return body, err
}

func (this *RequestContext[TReq, TRes]) respondWithAny(code int, payload any) {
	this.writer.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(this.writer).Encode(payload)
	if err != nil {
		this.writer.WriteHeader(http.StatusInternalServerError)
		this.logger.Error("Error encoding payload", "path", this.path, "status code", code, "payload", payload)
		
		return
	}

	this.writer.WriteHeader(code)
}

func (this *RequestContext[TReq, TRes]) RespondWithError(code int, err error) {
	this.respondWithAny(code, SafeError{
		Msg: err.Error(),
	})
}

func (this *RequestContext[TReq, TRes]) Respond(code int, res TRes) {
	this.respondWithAny(code, res)
}

// Automatically creates a Goroutine to listen in. Blocks until listener is ready
func (this *Listener) Listen() error {
	listener, err := net.Listen("tcp", ":"+fmt.Sprint(this.Port))
	this.listener = listener

	if err != nil {
		return err
	}

	// 10 = Base 10, 0 = auto match architecture's bit size
	port, err := strconv.ParseUint(strings.Split(listener.Addr().String(), ":")[1], 10, 0)
	this.Port = uint(port)

	// Separate mux to allow multiple services
	this.mux = http.NewServeMux()

	go func() {
		err := http.Serve(listener, this.mux)

		if err != nil {
			this.Logger.Error("Error when starting serving", "error", err)
		}
	}()

	return nil
}

func (this *Listener) RegisterRoute[TReq any, TRes any](path string, handler func(ctx RequestContext[TReq, TRes])) {
	handle := func(writer http.ResponseWriter, req *http.Request) {
		ctx := RequestContext[TReq, TRes]{
			path: path,
			Req: req,
			writer: writer,
			logger: this.Logger,
		}

		handler(ctx)
	}

	this.mux.HandleFunc(EXCHANGE_PATH, handle)
}