package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	store := NewStore()

	err := store.Set("foo", []byte("bar"))
	require.NoError(t, err)

	value, err := store.Get("foo")
	require.NoError(t, err)
	require.Equal(t, []byte("bar"), value)
}

func TestEmpty(t *testing.T) {
	store := NewStore()

	_, err := store.Get("foo")
	require.ErrorIs(t, err, ErrNoSuchKey)
}

func TestAscend(t *testing.T) {
	store, err := NewStoreWithData(map[string][]byte{
		"c":  []byte("c val"),
		"b":  []byte("b val"),
		"b1": []byte("b1 val"),
		"d":  []byte("d val"),
		"a":  []byte("a val"),
	})
	require.NoError(t, err)

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
	store, err := NewStoreWithData(map[string][]byte{
		"a":  []byte("a val"),
		"b":  []byte("b val"),
		"b1": []byte("b1 val"),
		"c":  []byte("c val"),
		"d":  []byte("d val"),
	})
	require.NoError(t, err)

	pairs := store.Between("b", "d")
	require.Equal(t, 3, len(pairs))

	require.Equal(t, Pair{Key: "b", Value: []byte("b val")}, pairs[0])
	require.Equal(t, Pair{Key: "b1", Value: []byte("b1 val")}, pairs[1])
	require.Equal(t, Pair{Key: "c", Value: []byte("c val")}, pairs[2])
}

func TestStore_Delete(t *testing.T) {
	store, err := NewStoreWithData(map[string][]byte{
		"a": []byte("a val"),
		"b": []byte("b val"),
	})
	require.NoError(t, err)

	err = store.Delete("a")
	require.NoError(t, err)

	aVal, err := store.Get("a")
	require.NoError(t, err)
	require.Nil(t, aVal)

	bVal, err := store.Get("b")
	require.NoError(t, err)
	require.Equal(t, "b val", string(bVal))

	// Test that setting the key again works.
	err = store.Set("a", []byte("new a val"))
	require.NoError(t, err)

	aVal, err = store.Get("a")
	require.NoError(t, err)
	require.Equal(t, "new a val", string(aVal))
}

func TestStore_Delete_NotExists(t *testing.T) {
	// Tests that deleting a key that does not exist in the store tombstones the key.

	store, err := NewStoreWithData(map[string][]byte{
		"b": []byte("b val"),
	})
	require.NoError(t, err)

	err = store.Delete("a")
	require.NoError(t, err)

	aVal, err := store.Get("a")
	require.NoError(t, err)
	require.Nil(t, aVal)

	bVal, err := store.Get("b")
	require.NoError(t, err)
	require.Equal(t, "b val", string(bVal))
}
