package paxocache

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
)

type Listener struct {
	Port       uint
	Token      string
	Logger *slog.Logger

	// DO NOT ACCESS BEFORE CALLING .Listen()!
	listener net.Listener
	mux *http.ServeMux
}

// Automatically creates a Goroutine to listen in. Blocks until listener is ready
func (this *Listener) Listen() error {
	listener, err := net.Listen("tcp", ":"+fmt.Sprint(this.Port))
	this.listener = listener
	if err != nil {
		return err
	}

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