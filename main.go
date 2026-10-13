package main

import (
	"fmt"
	"log/slog"
	"os"
	paxocache "paxocache/src"
)

func main() {
	fmt.Println("Creating service...")

	options := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &options))

	listener := paxocache.Listener{
		Port: 8000,
		Token: "abc",
		Logger: logger,
	}
	serviceDiscovery := paxocache.NewServerDiscoveryService(logger)

	err := listener.Listen()

	serviceDiscovery.RegisterListener(&listener)

	err = serviceDiscovery.ExchangeWith("http://127.0.0.1:8000")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Exiting...")
}