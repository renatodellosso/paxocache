package tests

import (
	"log/slog"
	"os"
	paxocache "paxocache/src"

	mapset "github.com/deckarep/golang-set/v3"
	"github.com/stretchr/testify/mock"
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
	MockGet    func(url string, path string, payload any, result any) (int, error)
	MockPost   func(url string, path string, payload any, result any) (int, error)
	MockPatch  func(url string, path string, payload any, result any) (int, error)
	MockPut    func(url string, path string, payload any, result any) (int, error)
	MockDelete func(url string, path string, payload any, result any) (int, error)
}

func (this *MockHttpClient) Get(url string, path string, payload any, result any) (int, error) {
	return this.MockGet(url, path, payload, result)
}

func (this *MockHttpClient) Post(url string, path string, payload any, result any) (int, error) {
	return this.MockPost(url, path, payload, result)
}

func (this *MockHttpClient) Patch(url string, path string, payload any, result any) (int, error) {
	return this.MockPatch(url, path, payload, result)
}

func (this *MockHttpClient) Put(url string, path string, payload any, result any) (int, error) {
	return this.MockPut(url, path, payload, result)
}

func (this *MockHttpClient) Delete(url string, path string, payload any, result any) (int, error) {
	return this.MockDelete(url, path, payload, result)
}

type MockPaxosPersistor[TOp comparable, TRes any] struct {
	mock.Mock
}

func (this *MockPaxosPersistor[TOp, TRes]) WriteSingleData(data *paxocache.PaxosSingleData) error {
	return this.Called(data).Error(0)
}
	
func (this *MockPaxosPersistor[TOp, TRes]) AppendPoolOp(op *paxocache.PoolChangeOp) error {
	return this.Called(op).Error(0)
}

func (this *MockPaxosPersistor[TOp, TRes]) AppendAcceptance(slot *paxocache.Slot[TOp]) error {
	return this.Called(slot).Error(0)
}
	
func (this *MockPaxosPersistor[TOp, TRes]) AppendChosen(slot *paxocache.ChosenSlot[TOp]) error {
	return this.Called(slot).Error(0)
}
	
func (this *MockPaxosPersistor[TOp, TRes]) HasData() (bool, error) {
	args := this.Called()
	return args.Bool(0), args.Error(1)
}
	
func (this *MockPaxosPersistor[TOp, TRes]) Recover() (*paxocache.PaxosPersistentData[TOp, TRes], error) {
	args := this.Called()
	return args.Get(0).(*paxocache.PaxosPersistentData[TOp, TRes]), args.Error(1)
}

var _ paxocache.PaxosPersistor[any, any] = &MockPaxosPersistor[any, any]{}

func CreateMockRootPersistor() (*MockPaxosPersistor[paxocache.PartitionOperation, mapset.Set[string]], func() paxocache.PaxosPersistor[paxocache.PartitionOperation, mapset.Set[string]]) {
	persistor := MockPaxosPersistor[paxocache.PartitionOperation, mapset.Set[string]]{}
	return &persistor, func() paxocache.PaxosPersistor[paxocache.PartitionOperation, mapset.Set[string]] {
		return &persistor
	}
}

func CreateMockPartitionPersistor() (*MockPaxosPersistor[any, any], func(partition string) paxocache.PaxosPersistor[any, any]) {
	persistor := MockPaxosPersistor[any, any]{}
	return &persistor, func(partition string) paxocache.PaxosPersistor[any, any] {
		return &persistor
	}
}