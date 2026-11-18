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
																			WithLogger(tests.DebugLogger())

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
	require.NotNil(t, val)
	assert.Equal(t, expected, *val)
}

func TestCacheSetAndGetTwoNodes(t *testing.T) {
	cacheA := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.SilentLogger())

	require.Nil(t, cacheA.Init())

	cacheB := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.SilentLogger()).
																			WithKnownHost(cacheA.GetAddress())

	require.Nil(t, cacheB.Init())

	key := "test"
	expectedA := 123
	expectedB := 321

	// A -> B
	require.Nil(t, cacheA.Set(key, &expectedA))

	val, err := cacheB.Get[int](key)

	assert.Nil(t, err)
	require.NotNil(t, val)
	assert.Equal(t, expectedA, *val)

	// B -> A
	require.Nil(t, cacheB.Set(key, &expectedB))

	val, err = cacheA.Get[int](key)

	assert.Nil(t, err)
	require.NotNil(t, val)
	assert.Equal(t, expectedB, *val)
}

func TestCacheCreatingPartitionClonesItOnOtherNodes(t *testing.T) {
	cacheA := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.DebugLogger())

	require.Nil(t, cacheA.Init())

	cacheB := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.SilentLogger()).
																			WithKnownHost(cacheA.GetAddress())

	require.Nil(t, cacheB.Init())

	key := "key"
	val := 123
	
	require.Nil(t, cacheA.Set(key, &val))

	assert.ElementsMatch(t, cacheB.GetKnownKeys().ToSlice(), cacheB.GetLocalKeys().ToSlice())
	assert.Contains(t, cacheB.GetLocalKeys().ToSlice(), key)
}

func TestCacheContinuesIfNodeFailsBeforeSet(t *testing.T) {
	cacheA := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.SilentLogger())

	require.Nil(t, cacheA.Init())

	cacheB := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.SilentLogger()).
																			WithKnownHost(cacheA.GetAddress())

	require.Nil(t, cacheB.Init())

	cacheC := paxocache.NewCache().
																			WithBasePath(t.TempDir()).
																			WithLogger(tests.SilentLogger()).
																			WithKnownHost(cacheA.GetAddress())
	require.Nil(t, cacheC.Init())

	require.Nil(t, cacheA.Shutdown())

	key := "key"
	val := 123

	require.Nil(t, cacheB.Set(key, &val))

	actual, err := cacheC.Get[int](key)
	require.Nil(t, err)
	assert.Equal(t, val, *actual)
}