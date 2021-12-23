package sstable

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/antw/violin/internal/storage"
)

func TestIterator(t *testing.T) {
	store, err := storage.NewStoreWithData(map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
	})
	require.NoError(t, err)

	iterator := storeIterator{store: store}

	kv, ok := iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "a", Value: []byte("1")}, kv)

	kv, ok = iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "b", Value: []byte("2")}, kv)

	kv, ok = iterator.Next()
	require.False(t, ok)
	require.Nil(t, kv)
}

func TestEmptyIterator(t *testing.T) {
	store := storage.NewStore()

	iterator := storeIterator{store: store}

	kv, ok := iterator.Next()
	require.False(t, ok)
	require.Nil(t, kv)
}
