package sstable

import (
	"bufio"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile("", "index_test")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()

	writer := indexWriter{bufio.NewWriter(f)}

	pairs := []struct {
		key string
		pos uint64
	}{
		{"foo", 0},
		{"bar", 10},
		{"baz", 512},
		{"qux", 1024},
	}

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
	require.Equal(t, uint64(0), pos)
}
