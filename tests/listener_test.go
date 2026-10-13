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

func TestListenerSetsPort(t *testing.T) {
	listener := paxocache.Listener{
		Logger: SilentLogger(),
	}

	assert.Equal(t, uint(0), listener.Port)
	listener.Listen()
	assert.NotEqual(t, 0, listener.Port)
}