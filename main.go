package main

import (
	"fmt"
	paxocache "paxocache/src"
	"time"
)

func main() {
	fmt.Println("Creating service...")

	listener, err := paxocache.NewServerDiscoveryService("")
	if err != nil {
		fmt.Printf("Error creating service: %v\n", err)
		return
	}

	listenChan := listener.Listen()

	// Print events
	go func() {
		for val := range listenChan {
			fmt.Println(val)
		}

		fmt.Println("Channel closed")
	}()

	time.Sleep(time.Second)

	sender, err := paxocache.NewServerDiscoveryService("")

	err = sender.RegisterWith("http://127.0.0.1:8080")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Exiting...")
}