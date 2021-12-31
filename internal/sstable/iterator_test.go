package sstable

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/antw/violin/internal/storage"
)

func TestIterator_WithAscend(t *testing.T) {
	store, err := storage.NewStoreWithData(map[string][]byte{
		"foo": []byte("1"),
		"bar": []byte("2"),
	})
	require.NoError(t, err)

	iterator := NewIterator(store.Ascend)

	kv, ok := iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "bar", Value: []byte("2")}, kv)

	kv, ok = iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "foo", Value: []byte("1")}, kv)

	kv, ok = iterator.Next()
	require.False(t, ok)
	require.Nil(t, kv)
}

func TestIterator_WithAscendBetween(t *testing.T) {
	store, err := storage.NewStoreWithData(map[string][]byte{
		"foo": []byte("1"),
		"bar": []byte("2"),
		"baz": []byte("3"),
	})
	require.NoError(t, err)

	iterator := NewIterator(func(it storage.Iterator) {
		store.AscendRange("bar", "foo", it)
	})

	kv, ok := iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "bar", Value: []byte("2")}, kv)

	kv, ok = iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "baz", Value: []byte("3")}, kv)

	kv, ok = iterator.Next()
	require.False(t, ok)
	require.Nil(t, kv)
}
