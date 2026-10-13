package paxocache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

type HttpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type HttpClient struct {
	token string
	Logger *slog.Logger
	Client HttpDoer
}

func Http(token string, logger *slog.Logger) *HttpClient {
	return &HttpClient{
		token: token,
		Logger: logger,
		Client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (this *HttpClient) request[TReq any, TRes any](method string, url string, path string, payload TReq) (*TRes, int, error) {
	if url == "" {
		return nil, http.StatusNotFound, errors.New("URL cannot be ''!")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // Release when done

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	req, err := http.NewRequestWithContext(ctx, method, url + path, bytes.NewReader(body))
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	req.Header.Set("User-Agent", "Go-HTTP-Client/1.1")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Bearer-Token", this.token)

	// Execute req
	res, err := this.Client.Do(req)

	this.Logger.Debug("Received response", "method", method, "url", url, "path", path, "status code", res.StatusCode)

	if err != nil {
		return nil, res.StatusCode, err
	}

	defer res.Body.Close()

	if res.StatusCode >= 400 {
		var decodedError SafeError
		err = json.NewDecoder(res.Body).Decode(&decodedError)

		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return nil, res.StatusCode, decodedError
	}

	var resBody TRes
	err = json.NewDecoder(res.Body).Decode(&resBody)
	if err != nil {
		return nil, res.StatusCode, err
	}

	return &resBody, res.StatusCode, nil
}

func (this *HttpClient) Get[TReq any, TRes any](url string, path string, payload TReq) (*TRes, int, error) {
	return this.request[TReq, TRes](http.MethodGet, url, path, payload)
}

func (this *HttpClient) Post[TReq any, TRes any](url string, path string, payload TReq) (*TRes, int, error) {
	return this.request[TReq, TRes](http.MethodPost, url, path, payload)
}

func (this *HttpClient) Patch[TReq any, TRes any](url string, path string, payload TReq) (*TRes, int, error) {
	return this.request[TReq, TRes](http.MethodPatch, url, path, payload)
}

func (this *HttpClient) Put[TReq any, TRes any](url string, path string, payload TReq) (*TRes, int, error) {
	return this.request[TReq, TRes](http.MethodPut, url, path, payload)
}

func (this *HttpClient) Delete[TReq any, TRes any](url string, path string, payload TReq) (*TRes, int, error) {
	return this.request[TReq, TRes](http.MethodDelete, url, path, payload)
}