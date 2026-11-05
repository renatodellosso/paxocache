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
	keyCounts := []int{100}
	nodeCounts := []int{1, 5}

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
					err := caches[0].Set(key, &val)

					if err != nil {
						b.Errorf("%v", err)
					}
				}

				// Warm keys
				for _, key := range keys {
					for _, cache := range caches {
						_, err := cache.Get[int](key)

						if err != nil {
							b.Errorf("%v", err)
						}
					}
				}

				b.ResetTimer()
				b.RunParallel(func(p *testing.PB) {
					keyIndex := 0
					nodeIndex := 0

					for p.Next() {
						_, err := caches[nodeIndex].Get[int](keys[keyIndex])

						if err != nil {
							b.Errorf("%v", err)
						}

						keyIndex = (keyIndex + 1) % keyCount
						if keyIndex == 1 {
							// Increment such that we hit every node
							nodeIndex = (nodeIndex + 1) % nodeCount
						}
					}
				})
				
			})
		}
	}
}

func BenchmarkCacheSet(b *testing.B) {
	keyCounts := []int{100}
	nodeCounts := []int{2, 5}

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
					err := caches[0].Set(key, &val)

					if err != nil {
						b.Errorf("%v", err)
					}
				}

				// Warm keys
				for _, key := range keys {
					for _, cache := range caches {
						_, err := cache.Get[int](key)

						if err != nil {
							b.Errorf("%v", err)
						}
					}
				}

				val := 123

				b.ResetTimer()
				b.RunParallel(func(p *testing.PB) {
					keyIndex := 0
					nodeIndex := 0

					for p.Next() {
						err := caches[nodeIndex].Set(keys[keyIndex], &val)
						if err != nil {
							b.Errorf("%v", err)
						}

						keyIndex = (keyIndex + 1) % keyCount
						if keyIndex == 1 {
							// Increment such that we hit every node
							nodeIndex = (nodeIndex + 1) % nodeCount
						}
					}
				})
			})
		}
	}
}