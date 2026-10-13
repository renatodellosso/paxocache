package tests

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	paxocache "paxocache/src"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeBodyDecodes(t *testing.T) {
	obj := Dummy{
		X: 99,
	}

	body, _ := json.Marshal(obj)

	req := http.Request{
		Body: io.NopCloser(bytes.NewReader(body)),
	}

	ctx := paxocache.RequestContext[Dummy, Dummy]{
		Req: &req,
	}

	decoded, err := ctx.DecodeBody()

	assert.Nil(t, err)
	assert.Equal(t, obj, decoded)
}

func TestListenerSetsPortWhenNotProvided(t *testing.T) {
	listener := paxocache.Listener{
		Logger: SilentLogger(),
	}

	assert.Equal(t, uint64(0), listener.Port)

	err := listener.Listen()

	assert.Nil(t, err)
	assert.NotEqual(t, uint64(0), listener.Port)
}

func TestListenerDoesNotOverridePortWhenProvided(t *testing.T) {
	listener := paxocache.Listener{
		Logger: SilentLogger(),
		Port: 8000,
	}

	err := listener.Listen()

	assert.Nil(t, err)
	assert.Equal(t, uint64(8000), listener.Port)
}

func TestListenerStartListeningSuccessfully(t *testing.T) {
	listener := paxocache.Listener{
		Logger: SilentLogger(),
	}

	err := listener.Listen()
	
	assert.Nil(t, err)
}