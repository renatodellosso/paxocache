package paxocache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"uuid"
)

type IHttpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type IHttpClient interface {
	Get(url string, path string, payload any, result any) (int, error)
	Post(url string, path string, payload any, result any) (int, error)
	Patch(url string, path string, payload any, result any) (int, error)
	Put(url string, path string, payload any, result any) (int, error)
	Delete(url string, path string, payload any, result any) (int, error)
}

type HttpClient struct {
	token string
	Logger *slog.Logger
	Client IHttpDoer
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

func sanitizeUrl(url string) string {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}

	return url
}

func (this *HttpClient) request(method string, url string, path string, payload any, result any) (int, error) {
	if url == "" {
		return http.StatusNotFound, errors.New("URL cannot be ''!")
	}

	url = sanitizeUrl(url)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // Release when done

	body, err := json.Marshal(payload)
	if err != nil {
		return http.StatusBadRequest, err
	}

	req, err := http.NewRequestWithContext(ctx, method, url + path, bytes.NewReader(body))
	if err != nil {
		return http.StatusBadRequest, err
	}

	req.Header.Set("User-Agent", "Go-HTTP-Client/1.1")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(REQUEST_ID_HEADER, uuid.NewV7().String())
	req.Header.Set(TOKEN_HEADER, this.token)

	// Execute req
	this.Logger.Debug("Sent request", "method", method, "url", url, "path", path, "request id", req.Header.Get(REQUEST_ID_HEADER))

	res, err := this.Client.Do(req)
	if err != nil {
		status := http.StatusInternalServerError
		if res != nil {
			status = res.StatusCode
		}
		return status, err
	}

	this.Logger.Debug("Received response", "method", method, "url", url, "path", path, "status code", res.StatusCode, "request id", req.Header.Get(REQUEST_ID_HEADER))

	defer res.Body.Close()

	if res.StatusCode >= 400 {
		var decodedError SafeError
		err = json.NewDecoder(res.Body).Decode(&decodedError)

		if err != nil {
			return http.StatusInternalServerError, err
		}

		return res.StatusCode, decodedError
	}

	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return res.StatusCode, err
	}

	return res.StatusCode, nil
}

func (this *HttpClient) Get(url string, path string, payload any, result any) (int, error) {
	return this.request(http.MethodGet, url, path, payload, result)
}

func (this *HttpClient) Post(url string, path string, payload any, result any) (int, error) {
	return this.request(http.MethodPost, url, path, payload, result)
}

func (this *HttpClient) Patch(url string, path string, payload any, result any) (int, error) {
	return this.request(http.MethodPatch, url, path, payload, result)
}

func (this *HttpClient) Put(url string, path string, payload any, result any) (int, error) {
	return this.request(http.MethodPut, url, path, payload, result)
}

func (this *HttpClient) Delete(url string, path string, payload any, result any) (int, error) {
	return this.request(http.MethodDelete, url, path, payload, result)
}