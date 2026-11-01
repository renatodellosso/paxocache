package benchmark

import (
	paxocache "paxocache/src"
	"paxocache/tests"
	"testing"
)

func BenchmarkCacheGetSingleKey(b *testing.B) {
	cache := paxocache.NewCache().
																			WithBasePath(b.TempDir()).
																			WithLogger(tests.SilentLogger())

	if cache.Init() != nil {
		b.FailNow()
	}

	key := "abc"
	val := 123

	if cache.Set(key, &val) != nil {
		b.FailNow()
	}

	for b.Loop() {
		cache.Get[int](key)
	}
}