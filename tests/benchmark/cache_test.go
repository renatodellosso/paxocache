package benchmark

import (
	"fmt"
	paxocache "paxocache/src"
	"paxocache/tests"
	"strconv"
	"testing"
)

func createCacheNetwork(b *testing.B, nodeCount int) ([]*paxocache.Cache, error) {
	res := []*paxocache.Cache{}

	for range nodeCount {
		var prev *paxocache.Cache
		if len(res) > 0 {
			prev = res[len(res)-1]
		}

		cache := paxocache.NewCache().
																			WithBasePath(b.TempDir()).
																			WithLogger(tests.SilentLogger())

		if prev != nil {
			cache.WithKnownHost(prev.GetAddress())
		}

		err := cache.Init()
		if err != nil {
			return nil, err
		}

		res = append(res, cache)
	}

	return res, nil
}

func createKeyList(keyCount int) []string {
	res := []string{}

	for i := range keyCount {
		res = append(res, strconv.Itoa(i))
	}

	return res
}


func BenchmarkCacheGet(b *testing.B) {
	keyCounts := []int{1, 5, 100, 1000}
	nodeCounts := []int{1, 2, 3, 5}

	for _, keyCount := range keyCounts {
		keys := createKeyList(keyCount)

		for _, nodeCount := range nodeCounts {
			b.Run(fmt.Sprintf("keys:%d,nodes:%d", keyCount, nodeCount), func(b *testing.B) {
				caches, err := createCacheNetwork(b, nodeCount)
				if err != nil {
					b.FailNow()
				}

				for _, key := range keys {
					val := 123
					caches[0].Set(key, &val)
				}

				keyIndex := 0
				nodeIndex := 0

				// Warm keys
				for _, key := range keys {
					for _, cache := range caches {
						cache.Get[int](key)
					}
				}

				for b.Loop() {
					caches[nodeIndex].Get[int](keys[keyIndex])

					keyIndex = (keyIndex + 1) % keyCount
					if keyIndex == 1 {
						// Increment such that we hit every node
						nodeIndex = (nodeIndex + 1) % nodeCount
					}
				}
			})
		}
	}
}

func BenchmarkCacheSet(b *testing.B) {
	keyCounts := []int{1, 5, 100, 1000}
	nodeCounts := []int{1, 2, 3, 5}

	for _, keyCount := range keyCounts {
		keys := createKeyList(keyCount)

		for _, nodeCount := range nodeCounts {
			b.Run(fmt.Sprintf("keys:%d,nodes:%d", keyCount, nodeCount), func(b *testing.B) {
				caches, err := createCacheNetwork(b, nodeCount)
				if err != nil {
					b.FailNow()
				}

				for _, key := range keys {
					val := 123
					caches[0].Set(key, &val)
				}

				keyIndex := 0
				nodeIndex := 0

				// Warm keys
				for _, key := range keys {
					for _, cache := range caches {
						cache.Get[int](key)
					}
				}

				val := 123

				for b.Loop() {
					caches[nodeIndex].Set(keys[keyIndex], &val)

					keyIndex = (keyIndex + 1) % keyCount
					if keyIndex == 1 {
						// Increment such that we hit every node
						nodeIndex = (nodeIndex + 1) % nodeCount
					}
				}
			})
		}
	}
}