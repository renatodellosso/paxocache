package tests

import (
	"log/slog"
	"os"
	paxocache "paxocache/src"

	mapset "github.com/deckarep/golang-set/v3"
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

type MockPersistor struct {
	MockWriteSingleData  func(data *paxocache.PaxosSingleData) error
	MockAppendAcceptance func(slot *paxocache.Slot[any]) error
	MockAppendChosen     func(slot *paxocache.ChosenSlot[any]) error
	MockHasData          func() (bool, error)
	MockRecover          func() (*paxocache.PaxosPersistentData[any, any], error)
}

func NewMockPersistor() *MockPersistor {
	return &MockPersistor{
		MockWriteSingleData: func(*paxocache.PaxosSingleData) error {
			return nil
		},
		MockAppendAcceptance: func(*paxocache.Slot[any]) error {
			return nil
		},
		MockAppendChosen: func(*paxocache.ChosenSlot[any]) error {
			return nil
		},
		MockHasData: func() (bool, error) {
			return false, nil
		},
		MockRecover: func() (*paxocache.PaxosPersistentData[any, any], error) {
			return nil, nil
		},
	}
}

func NewMockPersistorWithPartition(partition string) paxocache.PaxosPersistor[any, any] {
	return NewMockPersistor()
}

// Verify we implement the interface
var _ paxocache.PaxosPersistor[any, any] = (*MockPersistor)(nil)

func (this *MockPersistor) WriteSingleData(data *paxocache.PaxosSingleData) error {
	return this.MockWriteSingleData(data)
}

func (this *MockPersistor) AppendAcceptance(slot *paxocache.Slot[any]) error {
	return this.MockAppendAcceptance(slot)
}

func (this *MockPersistor) AppendChosen(slot *paxocache.ChosenSlot[any]) error {
	return this.MockAppendChosen(slot)
}

func (this *MockPersistor) HasData() (bool, error) {
	return this.MockHasData()
}

func (this *MockPersistor) Recover() (*paxocache.PaxosPersistentData[any, any], error) {
	return this.MockRecover()
}

type MockRootPersistor struct {
	MockWriteSingleData  func(data *paxocache.PaxosSingleData) error
	MockAppendAcceptance func(slot *paxocache.Slot[paxocache.PartitionOperation]) error
	MockAppendChosen     func(slot *paxocache.ChosenSlot[paxocache.PartitionOperation]) error
	MockHasData          func() (bool, error)
	MockRecover          func() (*paxocache.PaxosPersistentData[paxocache.PartitionOperation, mapset.Set[string]], error)
}

func NewMockRootPersistor() paxocache.PaxosPersistor[paxocache.PartitionOperation, mapset.Set[string]] {
	return &MockRootPersistor{
		MockWriteSingleData: func(*paxocache.PaxosSingleData) error {
			return nil
		},
		MockAppendAcceptance: func(*paxocache.Slot[paxocache.PartitionOperation]) error {
			return nil
		},
		MockAppendChosen: func(*paxocache.ChosenSlot[paxocache.PartitionOperation]) error {
			return nil
		},
		MockHasData: func() (bool, error) {
			return false, nil
		},
		MockRecover: func() (*paxocache.PaxosPersistentData[paxocache.PartitionOperation, mapset.Set[string]], error) {
			return nil, nil
		},
	}
}

// Verify we implement the interface
var _ paxocache.PaxosPersistor[paxocache.PartitionOperation, mapset.Set[string]] = (*MockRootPersistor)(nil)

func (this *MockRootPersistor) WriteSingleData(data *paxocache.PaxosSingleData) error {
	return this.MockWriteSingleData(data)
}

func (this *MockRootPersistor) AppendAcceptance(slot *paxocache.Slot[paxocache.PartitionOperation]) error {
	return this.MockAppendAcceptance(slot)
}

func (this *MockRootPersistor) AppendChosen(slot *paxocache.ChosenSlot[paxocache.PartitionOperation]) error {
	return this.MockAppendChosen(slot)
}

func (this *MockRootPersistor) HasData() (bool, error) {
	return this.MockHasData()
}

func (this *MockRootPersistor) Recover() (*paxocache.PaxosPersistentData[paxocache.PartitionOperation, mapset.Set[string]], error) {
	return this.MockRecover()
}