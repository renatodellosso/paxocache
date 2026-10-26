package unit

import (
	"encoding/json"
	"os"
	paxocache "paxocache/src"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func deleteDirIfExists(dir string) {
	os.RemoveAll(dir)
}

func TestWriteSingleDataWritesValidJson(t *testing.T) {
	dir := "/tmp/writeSingleData"

	defer deleteDirIfExists(dir)

	persistor := paxocache.FilePersistor[any, any]{
		BasePath: dir,
	}

	data := paxocache.PaxosSingleData{
		Id: "id",
		Partition: "abc",
		Leader: "leader",
		PromisedBallot: paxocache.Ballot{
			Number:	0, 
			OwnerId: "id",
		},
	}

	require.Nil(t, persistor.WriteSingleData(&data))

	file, err := os.ReadFile(dir + paxocache.SINGLE_DATA_FILE)
	require.Nil(t, err)

	var read paxocache.PaxosSingleData
	err = json.Unmarshal(file, &read)
	require.Nil(t, err)

	assert.Equal(t, data, read)
}

func TestAppendAcceptanceCreatesNewFile(t *testing.T) {
	dir := "/tmp/appendAcceptanceCreatesNewFile"

	defer deleteDirIfExists(dir)

	persistor := paxocache.FilePersistor[any, any]{
		BasePath: dir,
	}

	slot := paxocache.Slot[any]{
		Number: 0,
		Type: paxocache.ChosenSlotNoOp,
	}

	require.Nil(t, persistor.AppendAcceptance(&slot))

	file, err := os.ReadFile(dir + paxocache.ACCEPTED_FILE)
	require.Nil(t, err)

	var read paxocache.Slot[any]
	err = json.Unmarshal(file, &read)
	require.Nil(t, err)

	assert.Equal(t, slot, read)
}

func TestAppendAcceptanceAppendsToExisting(t *testing.T) {
	dir := "/tmp/appendAcceptanceAppendsToExisting"

	defer deleteDirIfExists(dir)

	persistor := paxocache.FilePersistor[any, any]{
		BasePath: dir,
	}

	slotA := paxocache.Slot[any]{
		Number: 0,
		Type: paxocache.ChosenSlotNoOp,
	}
	slotB := paxocache.Slot[any]{
		Number: 1,
		Type: paxocache.ChosenSlotNoOp,
	}

	require.Nil(t, persistor.AppendAcceptance(&slotA))
	require.Nil(t, persistor.AppendAcceptance(&slotB))

	file, err := os.ReadFile(dir + paxocache.ACCEPTED_FILE)
	require.Nil(t, err)

	text := string(file)

	jsonA, err := json.Marshal(slotA)
	require.Nil(t, err)
	jsonB, err := json.Marshal(slotB)
	require.Nil(t, err)

	assert.Equal(t, string(jsonA) + "\n" + string(jsonB) + "\n", text)
}

func TestAppendChosenCreatesNewFile(t *testing.T) {
	dir := "/tmp/appendChoseneCreatesNewFile"

	defer deleteDirIfExists(dir)

	persistor := paxocache.FilePersistor[any, any]{
		BasePath: dir,
	}

	slot := paxocache.ChosenSlot[any]{
		Number: 0,
		Type: paxocache.ChosenSlotNoOp,
	}

	require.Nil(t, persistor.AppendChosen(&slot))

	file, err := os.ReadFile(dir + paxocache.CHOSEN_FILE)
	require.Nil(t, err)

	var read paxocache.ChosenSlot[any]
	err = json.Unmarshal(file, &read)
	require.Nil(t, err)

	assert.Equal(t, slot, read)
}

func TestAppendChosenAppendsToExisting(t *testing.T) {
	dir := "/tmp/appendChosenAppendsToExisting"

	defer deleteDirIfExists(dir)

	persistor := paxocache.FilePersistor[any, any]{
		BasePath: dir,
	}

	slotA := paxocache.ChosenSlot[any]{
		Number: 0,
		Type: paxocache.ChosenSlotNoOp,
	}
	slotB := paxocache.ChosenSlot[any]{
		Number: 1,
		Type: paxocache.ChosenSlotNoOp,
	}

	require.Nil(t, persistor.AppendChosen(&slotA))
	require.Nil(t, persistor.AppendChosen(&slotB))

	file, err := os.ReadFile(dir + paxocache.CHOSEN_FILE)
	require.Nil(t, err)

	text := string(file)

	jsonA, err := json.Marshal(slotA)
	require.Nil(t, err)
	jsonB, err := json.Marshal(slotB)
	require.Nil(t, err)

	assert.Equal(t, string(jsonA) + "\n" + string(jsonB) + "\n", text)
}

func TestRecoverRecoverData(t *testing.T) {
	dir := "/tmp/recoverRecoversData"
	defer deleteDirIfExists(dir)

	data := paxocache.PaxosPersistentData[int, any]{
		Id: "id",
		Partition: "abc",
		Leader: "leader",
		PromisedBallot: paxocache.Ballot{
			Number:	1, 
			OwnerId: "id",
		},
		Acceptances: make(map[uint64]paxocache.Slot[int]),
		Chosen: make(map[uint64]paxocache.ChosenSlot[int]),
	}

	data.Acceptances[0] = paxocache.Slot[int]{
		Number: 0,
		Type: paxocache.ChosenSlotNoOp,
	}
	data.Acceptances[2] = paxocache.Slot[int]{
		Number: 2,
		Type: paxocache.ChosenSlotOperation,
		Operation: 123,
	}

	data.Chosen[1] = paxocache.ChosenSlot[int]{
		Number: 1,
		Type: paxocache.ChosenSlotNoOp,
	}
	data.Chosen[3] = paxocache.ChosenSlot[int]{
		Number: 3,
		Type: paxocache.ChosenSlotOperation,
		Operation: 321,
	}

	persistor := paxocache.FilePersistor[int, any]{
		BasePath: dir,
	}

	persistor.WriteSingleData(paxocache.SingleData(&data))

	for _, val := range data.Acceptances {
		persistor.AppendAcceptance(&val)
	}
	for _, val := range data.Chosen {
		persistor.AppendChosen(&val)
	}

	recovered, err := persistor.Recover()

	require.Nil(t, err)

	assert.Equal(t, &data, recovered)
}