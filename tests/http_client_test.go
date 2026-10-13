package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	paxocache "paxocache/src"
	"testing"

	"github.com/stretchr/testify/assert"
)

type CustomDoer struct {
	res *http.Response
}

func (this CustomDoer) Do(req *http.Request) (*http.Response, error) {
	return this.res, nil
}

func getClientWithResponse(code int, payload any) *paxocache.HttpClient {
	if code >= 400 {
		payload = paxocache.SafeError{
			Msg: payload.(error).Error(),
		}
	}

	body, _ := json.Marshal(payload)
	
	res := &http.Response{
		StatusCode: code,
		Header: make(http.Header),
		Body: io.NopCloser(bytes.NewReader(body)),
	}

	doer := CustomDoer{
		res,
	}

	return &paxocache.HttpClient{
		Logger: SilentLogger(),
		Client: doer,
	}
}

func TestReturnsPayload(t *testing.T) {
	body := Dummy{
		X: 1,
	}

	res, code, err := getClientWithResponse(200, body).Get[struct{}, Dummy]("localhost", "", struct{}{})

	assert.Nil(t, err)
	assert.Equal(t, 200, code)
	assert.Equal(t, body,  *res)
}

func TestReturnsError(t *testing.T) {
	err := errors.New("Test error")

	res, code, resErr := getClientWithResponse(500, err).Get[struct{}, Dummy]("localhost", "", struct{}{})

	assert.Nil(t, res)
	assert.Equal(t, 500, code)
	assert.Equal(t, err.Error(),  resErr.Error())
}