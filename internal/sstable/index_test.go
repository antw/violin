package sstable

import (
	"bufio"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	pairs := []indexEntry{
		{"foo", 0},
		{"bar", 10},
		{"baz", 512},
		{"qux", 1024},
	}

	index, teardown := createIndex(t, "index", pairs)
	defer teardown()

	for _, pair := range pairs {
		pos, ok := index.get(pair.key)

		require.True(t, ok)
		require.Equal(t, pair.pos, pos)
	}
}

func TestIndexNoKey(t *testing.T) {
	f, err := ioutil.TempFile("", "index_no_key_test")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()

	index, err := newIndex(f)
	require.NoError(t, err)

	pos, ok := index.get("not-exist")
	require.False(t, ok)
	require.Equal(t, uint32(0), pos)
}

func TestIndex_Ascend(t *testing.T) {
	index, teardown := createIndex(t, "index_ascend", []indexEntry{
		{"foo", 0},
		{"bar", 10},
		{"baz", 512},
		{"qux", 1024},
	})
	defer teardown()

	keys := make([]string, 0, 4)
	values := make([]uint32, 0, 4)

	index.Ascend(func(key string, pos uint32) bool {
		keys = append(keys, key)
		values = append(values, pos)
		return true
	})

	require.Equal(t, []string{"bar", "baz", "foo", "qux"}, keys)
	require.Equal(t, []uint32{10, 512, 0, 1024}, values)
}

func TestIndex_Ascend_EarlyStop(t *testing.T) {
	// Asserts that returning false in the iterator stops iteration.
	index, teardown := createIndex(t, "index_ascend", []indexEntry{
		{"foo", 0},
		{"bar", 10},
		{"baz", 512},
		{"qux", 1024},
	})
	defer teardown()

	keys := make([]string, 0, 3)
	values := make([]uint32, 0, 3)

	index.Ascend(func(key string, pos uint32) bool {
		keys = append(keys, key)
		values = append(values, pos)

		return key < "foo"
	})

	require.Equal(t, []string{"bar", "baz", "foo"}, keys)
	require.Equal(t, []uint32{10, 512, 0}, values)
}

func TestIndex_AscendRange(t *testing.T) {
	index, teardown := createIndex(t, "index_ascend_range", []indexEntry{
		{"foo", 0},
		{"bar", 10},
		{"baz", 512},
		{"qux", 1024},
	})
	defer teardown()

	keys := make([]string, 0, 2)
	values := make([]uint32, 0, 2)

	index.AscendRange("baz", "qux", func(key string, pos uint32) bool {
		keys = append(keys, key)
		values = append(values, pos)
		return true
	})

	require.Equal(t, []string{"baz", "foo"}, keys)
	require.Equal(t, []uint32{512, 0}, values)
}

func TestIndex_AscendRange_EarlyStop(t *testing.T) {
	// Asserts that returning false in the iterator stops iteration.
	index, teardown := createIndex(t, "index_ascend_range", []indexEntry{
		{"foo", 0},
		{"bar", 10},
		{"baz", 512},
		{"qux", 1024},
	})
	defer teardown()

	keys := make([]string, 0, 2)
	values := make([]uint32, 0, 2)

	index.AscendRange("bar", "qux", func(key string, pos uint32) bool {
		keys = append(keys, key)
		values = append(values, pos)

		return key < "baz"
	})

	require.Equal(t, []string{"bar", "baz"}, keys)
	require.Equal(t, []uint32{10, 512}, values)
}

// createIndex creates a new index with the given keys and positions, and returns it.
func createIndex(t *testing.T, name string, pairs []indexEntry) (*index, func()) {
	f, err := ioutil.TempFile("", name+"_test")
	require.NoError(t, err)

	writer := indexWriter{bufio.NewWriter(f)}

	for _, pair := range pairs {
		err = writer.Write(pair.key, pair.pos)
		require.NoError(t, err)
	}

	err = writer.Flush()
	require.NoError(t, err)

	_, err = f.Seek(0, 0)
	require.NoError(t, err)

	index, err := newIndex(f)
	require.NoError(t, err)

	return index, func() { _ = os.Remove(f.Name()) }
}
