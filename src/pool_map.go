package paxocache

import (
	"encoding/json"

	mapset "github.com/deckarep/golang-set/v3"
)

type PoolMap map[uint64]mapset.Set[string]

func (this *PoolMap) UnmarshalJSON(data []byte) error {
	// Decode into regular slices first

	var raw map[uint64][]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Preserve nil
	if raw == nil {
		*this = nil
		return nil
	}

	// Convert each entry, len(raw) is a capacity hint
	pools := make(PoolMap, len(raw))
	for slot, set := range raw {
		pools[slot] = mapset.NewSet(set...)
	}

	*this = pools
	return nil
}