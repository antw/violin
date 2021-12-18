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

func TestAscend(t *testing.T) {
	store := NewStore()

	kvs := []struct {
		key   string
		value []byte
	}{
		{key: "c", value: []byte("c val")},
		{key: "b", value: []byte("b val")},
		{key: "b1", value: []byte("b1 val")},
		{key: "d", value: []byte("d val")},
		{key: "a", value: []byte("a val")},
	}

	for _, kv := range kvs {
		err := store.Set(kv.key, kv.value)
		require.NoError(t, err)
	}

	index := 0
	expected := []string{"a", "b", "b1", "c", "d"}

	store.Ascend(func(key string, value []byte) bool {
		require.Equal(t, expected[index], key)
		require.Equal(t, []byte(expected[index]+" val"), value)

		index += 1
		return true
	})
}

func TestBetween(t *testing.T) {
	store := NewStore()

	kvs := []struct {
		key   string
		value []byte
	}{
		{key: "a", value: []byte("a val")},
		{key: "b", value: []byte("b val")},
		{key: "b1", value: []byte("b1 val")},
		{key: "c", value: []byte("c val")},
		{key: "d", value: []byte("d val")},
	}

	for _, kv := range kvs {
		err := store.Set(kv.key, kv.value)
		require.NoError(t, err)
	}

	pairs := store.Between("b", "d")
	require.Equal(t, 3, len(pairs))

	require.Equal(t, Pair{Key: "b", Value: []byte("b val")}, pairs[0])
	require.Equal(t, Pair{Key: "b1", Value: []byte("b1 val")}, pairs[1])
	require.Equal(t, Pair{Key: "c", Value: []byte("c val")}, pairs[2])
}
