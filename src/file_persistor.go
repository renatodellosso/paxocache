package paxocache

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"unicode"
)

const SINGLE_DATA_FILE = "/single.json"
const ACCEPTED_FILE = "/accepted.json"
const CHOSEN_FILE = "/chosen.json"

type FilePersistor[TOp comparable, TRes any] struct{
	BasePath string
}

func (this FilePersistor[TOp, TRes]) initPath() error {
	// 755 is owner full access, everyone else read access
	return os.MkdirAll(this.BasePath, 0o755)
}

func (this FilePersistor[TOp, TRes]) WriteSingleData(data *PaxosSingleData) error {
	err := this.initPath()
	if err != nil {
		return err
	}

	file, err := os.Create(this.BasePath + SINGLE_DATA_FILE)
	if err != nil {
		return err
	}

	defer file.Close()

	encoder := json.NewEncoder(file)

	err = encoder.Encode(data)
	if err != nil {
		return err
	}

	return nil
}

func (this FilePersistor[TOp, TRes]) AppendAcceptance(slot *Slot[TOp]) error {
	err := this.initPath()
	if err != nil {
		return err
	}

	file, err := os.OpenFile(this.BasePath + ACCEPTED_FILE, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	encoder := json.NewEncoder(file)

	err = encoder.Encode(slot)
	if err != nil {
		return err
	}

	return nil
}

func (this FilePersistor[TOp, TRes]) AppendChosen(slot *ChosenSlot[TOp]) error {
	err := this.initPath()
	if err != nil {
		return err
	}

	file, err := os.OpenFile(this.BasePath + CHOSEN_FILE, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	encoder := json.NewEncoder(file)

	err = encoder.Encode(slot)
	if err != nil {
		return err
	}

	return nil
}

func (this FilePersistor[TOp, TRes]) HasData() (bool, error) {
	file, err := os.Open(this.BasePath + SINGLE_DATA_FILE)

	if err != nil {
		return false, err
	}

	file.Close()

	return true, err
}

func sliceToMap[T any](slice []T, getKey func(*T) uint64) map[uint64]T {
	res := make(map[uint64]T)

	for _, slot := range slice {
		res[getKey(&slot)] = slot
	}

	return res
}

func splitFileToJsonEntries[T any](path string) (*[]T, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	split := []string{}

	// Open brackets 
	open := 0

	for {
		char, _, err := reader.ReadRune()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		// Ensure we always have a string to work with
		if open == 0 {
			// Skip whitespace between entries
			if unicode.IsSpace(char) {
				continue
			}
			split = append(split, "")
		}

		switch char {
		case '{':
			open++
		case '}':
			open--
		}

		// We have to convert runes to strings explicitly
		split[len(split) - 1] += string(char)
	}

	res := make([]T, len(split))

	for index, str := range split {
		var data T
		err := json.Unmarshal([]byte(str), &data)

		if err != nil {
			return nil, err
		}

		res[index] = data
	}

	return &res, nil
}

// WIP
func (this FilePersistor[TOp, TRes]) Recover() (*PaxosPersistentData[TOp, TRes], error) {
	file, err := os.Open(this.BasePath + SINGLE_DATA_FILE)

	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data PaxosPersistentData[TOp, TRes]

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		return nil, err
	}

	slotSlice, err := splitFileToJsonEntries[Slot[TOp]](this.BasePath + ACCEPTED_FILE)
	if err != nil {
		return nil, err
	}

	data.Acceptances = sliceToMap(*slotSlice, func(slot *Slot[TOp]) uint64 {
		return slot.Number
	})

	chosenSlotSlice, err := splitFileToJsonEntries[ChosenSlot[TOp]](this.BasePath + CHOSEN_FILE)
	if err != nil {
		return nil, err
	}

	data.Chosen = sliceToMap(*chosenSlotSlice, func(slot *ChosenSlot[TOp]) uint64 {
		return slot.Number
	})

	return &data, nil
}

var _ PaxosPersistor[any, any] = FilePersistor[any, any]{}
