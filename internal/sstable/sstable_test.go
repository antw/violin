package sstable

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/antw/violin/internal/storage"
)

func TestWriter(t *testing.T) {
	store := storage.NewStore()

	err := store.Set("foo", []byte("bar"))
	require.NoError(t, err)

	err = store.Set("baz", []byte("qux"))
	require.NoError(t, err)

	writer, teardown := createWriter(t, store)
	defer teardown()

	indexPath := writer.indexFile.Name()
	dataPath := writer.kvFile.Name()

	err = writer.Write()
	require.NoError(t, err)

	// For now, verify that both files have _something_ written.
	kvStat, err := os.Stat(dataPath)
	require.NoError(t, err)
	require.True(t, kvStat.Size() > 0, "expected data file size to be non-zero")

	indexStat, err := os.Stat(indexPath)
	require.NoError(t, err)
	require.True(t, indexStat.Size() > 0, "expected index file size to be non-zero")
}

func TestSSTable(t *testing.T) {
	sstable, teardown := createSSTable(t)
	defer teardown()

	foo, ok := sstable.Get("foo")
	require.True(t, ok)
	require.Equal(t, []byte("bar"), foo)

	baz, ok := sstable.Get("baz")
	require.True(t, ok)
	require.Equal(t, []byte("qux"), baz)

	nope, ok := sstable.Get("nope")
	require.False(t, ok)
	require.Equal(t, []byte(nil), nope)
}

// -------------------------------------------------------------------------------------------------

func createWriter(t *testing.T, source SerializableStore) (Writer, func()) {
	os.TempDir()
	dataFile, err := os.CreateTemp("", "sstable_writer_data_test")
	require.NoError(t, err)

	indexFile, err := os.CreateTemp("", "sstable_writer_index_test")
	require.NoError(t, err)

	teardown := func() {
		_ = os.Remove(dataFile.Name())
		_ = os.Remove(indexFile.Name())
	}

	return Writer{
		kvFile:    dataFile,
		indexFile: indexFile,
		source:    source,
	}, teardown
}

// createSSTable creates an SSTable with two key/value pairs:
//
//   foo: bar
//   baz: qux
func createSSTable(t *testing.T) (*SSTable, func()) {
	store := storage.NewStore()
	err := store.Set("foo", []byte("bar"))
	require.NoError(t, err)

	err = store.Set("baz", []byte("qux"))
	require.NoError(t, err)

	writer, teardown := createWriter(t, store)
	err = writer.Write()
	require.NoError(t, err)

	kvFile, err := os.Open(writer.kvFile.Name())
	require.NoError(t, err)

	indexFile, err := os.Open(writer.indexFile.Name())
	require.NoError(t, err)

	sstable, err := NewSSTable(kvFile, indexFile)
	require.NoError(t, err)

	return sstable, teardown
}
