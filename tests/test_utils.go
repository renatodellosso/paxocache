package tests

import (
	"log/slog"
	"os"
)

type Dummy struct {
	// Must be uppercase to be serialized into JSON
	X int
}

func SilentLogger() *slog.Logger {
	options := slog.HandlerOptions{
		Level: slog.LevelError + 1,
	}
	
	return slog.New(slog.NewJSONHandler(os.Stdout, &options))
}

func DebugLogger() *slog.Logger {
	options := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	
	return slog.New(slog.NewJSONHandler(os.Stdout, &options))
}

type MockHttpClient struct {
	MockGet func(url string, path string, payload any, result any) (int, error)
	MockPost func(url string, path string, payload any, result any) (int, error)
	MockPatch func(url string, path string, payload any, result any) (int, error)
	MockPut func(url string, path string, payload any, result any) (int, error)
	MockDelete func(url string, path string, payload any, result any) (int, error)
}

func (this *MockHttpClient) Get(url string, path string, payload any, result any) (int, error) {
	return this.MockGet(url, path, payload, result)
}

func (this *MockHttpClient) Post(url string, path string, payload any, result any) (int, error) {
	return this.MockGet(url, path, payload, result)
}

func (this *MockHttpClient) Patch(url string, path string, payload any, result any) (int, error) {
	return this.MockGet(url, path, payload, result)
}

func (this *MockHttpClient) Put(url string, path string, payload any, result any) (int, error) {
	return this.MockGet(url, path, payload, result)
}

func (this *MockHttpClient) Delete(url string, path string, payload any, result any) (int, error) {
	return this.MockGet(url, path, payload, result)
}