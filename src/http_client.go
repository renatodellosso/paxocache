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

type HttpClient struct {
	token string
	logger *slog.Logger
}

func Http(token string, logger *slog.Logger) *HttpClient {
	return &HttpClient{
		token: token,
		logger: logger,
	}
}

func (this *HttpClient) request[TReq any, TRes any](method string, url string, path string, payload TReq) (TRes, error) {
	var nilEquiv TRes

	if url == "" {
		return nilEquiv, errors.New("URL cannot be ''!")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // Release when done

	body, err := json.Marshal(payload)
	if err != nil {
		return nilEquiv, err
	}

	req, err := http.NewRequestWithContext(ctx, method, url + path, bytes.NewReader(body))
	if err != nil {
		return nilEquiv, err
	}

	req.Header.Set("User-Agent", "Go-HTTP-Client/1.1")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Bearer-Token", this.token)

	client := &http.Client{
			Timeout: 10 * time.Second,
	}

	// Execute req
	res, err := client.Do(req)

	this.logger.Debug("Received response", "method", method, "url", url, "path", path, "status code", res.StatusCode)

	if err != nil {
		return nilEquiv, err
	}

	var resBody TRes
	err = json.NewDecoder(res.Body).Decode(&resBody)
	if err != nil {
		return nilEquiv, err
	}

	defer res.Body.Close()

	return resBody, nil
}

func (this *HttpClient) Get[TReq any, TRes any](url string, path string, payload TReq) (TRes, error) {
	return this.request[TReq, TRes](http.MethodGet, url, path, payload)
}

func (this *HttpClient) Post[TReq any, TRes any](url string, path string, payload TReq) (TRes, error) {
	return this.request[TReq, TRes](http.MethodPost, url, path, payload)
}

func (this *HttpClient) Patch[TReq any, TRes any](url string, path string, payload TReq) (TRes, error) {
	return this.request[TReq, TRes](http.MethodPatch, url, path, payload)
}

func (this *HttpClient) Put[TReq any, TRes any](url string, path string, payload TReq) (TRes, error) {
	return this.request[TReq, TRes](http.MethodPut, url, path, payload)
}

func (this *HttpClient) Delete[TReq any, TRes any](url string, path string, payload TReq) (TRes, error) {
	return this.request[TReq, TRes](http.MethodDelete, url, path, payload)
}