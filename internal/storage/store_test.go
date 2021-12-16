package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	store := NewStore()

	err := store.Set("foo", []byte("bar"))
	require.NoError(t, err)

	value, ok := store.Get("foo")
	require.True(t, ok)
	require.Equal(t, []byte("bar"), value)
}

func TestEmpty(t *testing.T) {
	store := NewStore()

	_, ok := store.Get("foo")
	require.False(t, ok)
}

func TestBetween(t *testing.T) {
	store := NewStore()

	kvs := []struct {
		key   string
		value []byte
	}{
		{key: "a", value: []byte("a")},
		{key: "b", value: []byte("b")},
		{key: "b1", value: []byte("b1")},
		{key: "c", value: []byte("c")},
		{key: "d", value: []byte("d")},
	}

	for _, kv := range kvs {
		err := store.Set(kv.key, kv.value)
		require.NoError(t, err)
	}

	pairs := store.Between("b", "c")
	require.Equal(t, 3, len(pairs))

	require.Equal(t, Pair{Key: "b", Value: []byte("b")}, pairs[0])
	require.Equal(t, Pair{Key: "b1", Value: []byte("b1")}, pairs[1])
	require.Equal(t, Pair{Key: "c", Value: []byte("c")}, pairs[2])
}
