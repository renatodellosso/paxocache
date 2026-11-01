package integration

import (
	paxocache "paxocache/src"
	"paxocache/tests"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheInits(t *testing.T) {
	cache := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.SilentLogger())

	assert.Nil(t, cache.Init())
	assert.NotNil(t, cache.GetAddress())
}

func TestCacheGetNonExisting(t *testing.T) {
	cache := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.SilentLogger())

	require.Nil(t, cache.Init())

	val, err := cache.Get[any]("test")

	assert.Nil(t, val)
	assert.Nil(t, err)
}

func TestCacheSetAndGet(t *testing.T) {
	cache := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.SilentLogger())

	require.Nil(t, cache.Init())

	key := "test"
	expected := 123

	require.Nil(t, cache.Set(key, &expected))

	val, err := cache.Get[int](key)

	assert.Nil(t, err)
	assert.Equal(t, expected, *val)
}