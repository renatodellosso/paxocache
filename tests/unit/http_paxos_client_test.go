package unit

import (
	paxocache "paxocache/src"
	"testing"
)

func TestFulfillsPaxosClientInterface(t *testing.T) {
	client := paxocache.HttpPaxosClient[any] {}
	var _ paxocache.PaxosClient[any] = &client
}