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

func TestWriterWithAggregator(t *testing.T) {
	first, err := storage.NewStoreWithData(map[string][]byte{
		"a": []byte("one"),
		"c": []byte("three"),
	})
	require.NoError(t, err)

	second, err := storage.NewStoreWithData(map[string][]byte{
		"b": []byte("two"),
	})
	require.NoError(t, err)

	agg := aggregator{stores: []storage.SerializableStore{first, second}}
	writer, writerTeardown := createWriter(t, &agg)
	defer writerTeardown()

	err = writer.Write()
	require.NoError(t, err)

	table, readerTeardown := openSSTable(t, writer.kvFile.Name(), writer.indexFile.Name())
	defer readerTeardown()

	a, err := table.Get("a")
	require.NoError(t, err)
	require.Equal(t, []byte("one"), a)

	b, err := table.Get("b")
	require.NoError(t, err)
	require.Equal(t, []byte("two"), b)

	c, err := table.Get("c")
	require.NoError(t, err)
	require.Equal(t, []byte("three"), c)
}

func TestSSTable(t *testing.T) {
	sstable, teardown := createSSTable(t, []*KeyValue{
		{Key: "foo", Value: []byte("bar")},
		{Key: "baz", Value: []byte("qux")},
	})
	defer teardown()

	foo, err := sstable.Get("foo")
	require.NoError(t, err)
	require.Equal(t, []byte("bar"), foo)

	baz, err := sstable.Get("baz")
	require.NoError(t, err)
	require.Equal(t, []byte("qux"), baz)

	nope, err := sstable.Get("nope")
	require.ErrorIs(t, err, storage.ErrNoSuchKey)
	require.Equal(t, []byte(nil), nope)
}

// -------------------------------------------------------------------------------------------------

func TestOpenSSTable(t *testing.T) {
	dataFile, indexFile, teardown := createTableFiles(t, "sstable_open")
	defer teardown()

	_, err := OpenSSTable(dataFile.Name(), indexFile.Name())

	require.NoError(t, err)
}

func TestOpenSSTableNotReadable(t *testing.T) {
	dataFile, indexFile, teardown := createTableFiles(t, "sstable_open_not_readable")
	defer teardown()

	err := os.Chmod(dataFile.Name(), 0200) // -w- --- ---
	require.NoError(t, err)

	_, err = OpenSSTable(dataFile.Name(), indexFile.Name())

	require.ErrorIs(t, err, os.ErrPermission)
}

func TestOpenSSTableNoData(t *testing.T) {
	indexFile, teardown := createTableFile(t, "sstable_open_no_data")
	defer teardown()

	_, err := OpenSSTable(indexFile.Name()+"nope", indexFile.Name())

	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestOpenSSTableNoIndex(t *testing.T) {
	dataFile, teardown := createTableFile(t, "sstable_open_no_index")
	defer teardown()

	_, err := OpenSSTable(dataFile.Name(), dataFile.Name()+"nope")

	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestSSTable_Ascend(t *testing.T) {
	table, teardown := createSSTable(t, []*KeyValue{
		{Key: "foo", Value: []byte("one")},
		{Key: "bar", Value: []byte("two")},
		{Key: "baz", Value: []byte("three")},
		{Key: "qux", Value: []byte("four")},
	})
	defer teardown()

	keys := make([]string, 0, 4)
	values := make([]string, 0, 4)

	table.Ascend(func(key string, value []byte) bool {
		keys = append(keys, key)
		values = append(values, string(value))

		return true
	})

	require.Equal(t, []string{"bar", "baz", "foo", "qux"}, keys)
	require.Equal(t, []string{"two", "three", "one", "four"}, values)
}

func TestSStable_Ascend_EarlyStop(t *testing.T) {
	// Asserts that returning false in the iterator stops iteration.

	table, teardown := createSSTable(t, []*KeyValue{
		{Key: "foo", Value: []byte("one")},
		{Key: "bar", Value: []byte("two")},
		{Key: "baz", Value: []byte("three")},
		{Key: "qux", Value: []byte("four")},
	})
	defer teardown()

	keys := make([]string, 0, 3)
	values := make([]string, 0, 3)

	table.Ascend(func(key string, value []byte) bool {
		keys = append(keys, key)
		values = append(values, string(value))

		return key < "foo"
	})

	require.Equal(t, []string{"bar", "baz", "foo"}, keys)
	require.Equal(t, []string{"two", "three", "one"}, values)
}

func TestSSTable_AscendRange(t *testing.T) {
	table, teardown := createSSTable(t, []*KeyValue{
		{Key: "foo", Value: []byte("one")},
		{Key: "bar", Value: []byte("two")},
		{Key: "baz", Value: []byte("three")},
		{Key: "qux", Value: []byte("four")},
	})
	defer teardown()

	keys := make([]string, 0, 2)
	values := make([]string, 0, 2)

	table.AscendRange("baz", "qux", func(key string, value []byte) bool {
		keys = append(keys, key)
		values = append(values, string(value))

		return true
	})

	require.Equal(t, []string{"baz", "foo"}, keys)
	require.Equal(t, []string{"three", "one"}, values)
}

func TestSSTable_AscendRange_EarlyStop(t *testing.T) {
	// Asserts that returning false in the iterator stops iteration.

	table, teardown := createSSTable(t, []*KeyValue{
		{Key: "foo", Value: []byte("one")},
		{Key: "bar", Value: []byte("two")},
		{Key: "baz", Value: []byte("three")},
		{Key: "qux", Value: []byte("four")},
	})
	defer teardown()

	keys := make([]string, 0, 2)
	values := make([]string, 0, 2)

	table.AscendRange("bar", "qux", func(key string, value []byte) bool {
		keys = append(keys, key)
		values = append(values, string(value))

		return key < "baz"
	})

	require.Equal(t, []string{"bar", "baz"}, keys)
	require.Equal(t, []string{"two", "three"}, values)
}

// -------------------------------------------------------------------------------------------------

func createTableFile(t *testing.T, pattern string) (*os.File, func()) {
	file, err := os.CreateTemp("", pattern)
	require.NoError(t, err)

	return file, func() { _ = os.Remove(file.Name()) }
}

func createTableFiles(t *testing.T, pattern string) (*os.File, *os.File, func()) {
	data, dTeardown := createTableFile(t, pattern+"_data")
	index, iTeardown := createTableFile(t, pattern+"_index")

	return data, index, func() {
		dTeardown()
		iTeardown()
	}
}

func createWriter(t *testing.T, source storage.SerializableStore) (Writer, func()) {
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

// createSSTable creates an SSTable with the given key-value pairs.
func createSSTable(t *testing.T, kvs []*KeyValue) (*SSTable, func()) {
	store := storage.NewStore()

	for _, kv := range kvs {
		err := store.Set(kv.Key, kv.Value)
		require.NoError(t, err)
	}

	writer, writerTeardown := createWriter(t, store)
	err := writer.Write()
	require.NoError(t, err)

	table, readerTeardown := openSSTable(t, writer.kvFile.Name(), writer.indexFile.Name())

	return table, func() {
		writerTeardown()
		readerTeardown()
	}
}

// openSSTable opens an existing SSTable using paths to the data and index.
func openSSTable(t *testing.T, dataPath, indexPath string) (*SSTable, func()) {
	table, err := OpenSSTable(dataPath, indexPath)
	require.NoError(t, err)

	return table, func() { _ = table.Close() }
}
