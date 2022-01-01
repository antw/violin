package server

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/antw/violin/internal/sstable"
	"github.com/antw/violin/internal/storage"
)

func TestController_SetGet(t *testing.T) {
	conf, confTeardown := defaultControllerConfig(t, "set_get")
	defer confTeardown()
	c := NewController([]storage.ReadableStore{}, conf)

	err := c.Set("foo", []byte("bar val"))
	require.NoError(t, err)
	require.Equal(t, 10, int(c.estimatedSize))

	err = c.Set("baz", []byte("qux val"))
	require.NoError(t, err)
	require.Equal(t, 20, int(c.estimatedSize))

	val, err := c.Get("foo")
	require.NoError(t, err)
	require.Equal(t, "bar val", string(val))

	err = c.Set("baz", []byte("qux"))
	require.NoError(t, err)
	require.Equal(t, 16, int(c.estimatedSize))
}

func TestController_GetFromReadable(t *testing.T) {
	older, err := storage.NewStoreWithData(map[string][]byte{
		"foo": []byte("bar"),
		"baz": []byte("qux"),
	})
	require.NoError(t, err)

	newer, err := storage.NewStoreWithData(map[string][]byte{
		"foo":  []byte("bar new"),
		"quux": []byte("quuz"),
	})
	require.NoError(t, err)

	conf, confTeardown := defaultControllerConfig(t, "get_from_readable")
	defer confTeardown()
	c := NewController([]storage.ReadableStore{older, newer}, conf)

	// New value in newer store
	foo, err := c.Get("foo")
	require.NoError(t, err)
	require.Equal(t, "bar new", string(foo))

	// Value from older store
	baz, err := c.Get("baz")
	require.NoError(t, err)
	require.Equal(t, "qux", string(baz))

	// Value from newer store
	quux, err := c.Get("quux")
	require.NoError(t, err)
	require.Equal(t, "quuz", string(quux))
}

func TestController_Delete(t *testing.T) {
	conf, confTeardown := defaultControllerConfig(t, "delete")
	defer confTeardown()
	c := NewController([]storage.ReadableStore{}, conf)

	err := c.Set("foo", []byte("bar val"))
	require.NoError(t, err)

	err = c.Set("baz", []byte("qux val"))
	require.NoError(t, err)

	require.Equal(t, 20, int(c.estimatedSize))

	err = c.Delete("foo")
	require.NoError(t, err)

	// Key still contributes to the size.
	require.Equal(t, 13, int(c.estimatedSize))
}

func TestController_flushAuto(t *testing.T) {
	conf, confTeardown := configWithDir(
		t,
		ControllerConfig{
			FlushBytes:     1,
			FlushFrequency: 50 * time.Millisecond,
		},
		"flush_auto",
	)
	defer confTeardown()

	// Tests that the controller flushes the active store when the estimated store size exceeds the
	// configured value.
	c := NewController([]storage.ReadableStore{}, conf)
	c.Start()
	defer func() { _ = c.Close() }()

	// Start with no readable stores.
	require.Equal(t, 0, len(c.readableStores))

	err := c.Set("foo", []byte("bar"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.readableStores) == 1
	}, 1*time.Second, 25*time.Millisecond)
}

func TestController_flushActiveStore(t *testing.T) {
	conf, confTeardown := defaultControllerConfig(t, "flush")
	defer confTeardown()
	c := NewController([]storage.ReadableStore{}, conf)

	// Start with no readable stores.
	require.Equal(t, 0, len(c.readableStores))

	err := c.Set("foo", []byte("bar"))
	require.NoError(t, err)

	err = c.flushActiveStore()
	require.NoError(t, err)

	// Flush should have cleared the active store and added a readable store.
	_, err = c.activeStore.Get("foo")
	require.ErrorIs(t, err, storage.ErrNoSuchKey)
	require.Equal(t, int64(0), c.estimatedSize)
	require.Equal(t, 1, len(c.readableStores))

	// Assert that the value can still be read from the SSTable.
	val, err := c.Get("foo")
	require.NoError(t, err)
	require.Equal(t, "bar", string(val))
}

func TestController_flushActiveStore_Overwrite(t *testing.T) {
	conf, confTeardown := defaultControllerConfig(t, "flush")
	defer confTeardown()

	writeSSTable(t, &conf, 1, map[string][]byte{
		"foo": []byte("bar"),
	})

	// Tests that a memory store which is flushed to an SSTable takes precedence over a value in an
	// existing table.
	c, err := LoadController(conf)
	require.NoError(t, err)

	// Start with one readable store.
	require.Equal(t, 1, len(c.readableStores))

	err = c.Set("foo", []byte("baz"))
	require.NoError(t, err)

	err = c.flushActiveStore()
	require.NoError(t, err)

	// Flush should have cleared the active store and added a readable store.
	_, err = c.activeStore.Get("foo")
	require.ErrorIs(t, err, storage.ErrNoSuchKey)
	require.Equal(t, 2, len(c.readableStores))

	require.Equal(t, int64(0), c.estimatedSize)
	require.Equal(t, 0, c.activeStore.Len())

	// Assert that the new table is read from the newer SSTable.
	val, err := c.Get("foo")
	require.NoError(t, err)
	require.Equal(t, "baz", string(val))
}

func TestLoadController(t *testing.T) {
	config, confTeardown := defaultControllerConfig(t, "with_stores")
	defer confTeardown()

	teardown := writeSSTable(t, &config, 1, map[string][]byte{
		"foo": []byte("bar"),
		"baz": []byte("qux"),
	})
	defer teardown()

	c, err := LoadController(config)
	require.NoError(t, err)

	val, err := c.Get("foo")
	require.NoError(t, err)
	require.Equal(t, "bar", string(val))
}

// -------------------------------------------------------------------------------------------------

func defaultControllerConfig(t *testing.T, pattern string) (ControllerConfig, func()) {
	return configWithDir(t, ControllerConfig{}, pattern)
}

func configWithDir(t *testing.T, conf ControllerConfig, pattern string) (ControllerConfig, func()) {
	path, err := ioutil.TempDir("", "test_controller_"+pattern)
	require.NoError(t, err)

	conf.Dir = path

	return conf, func() { _ = os.RemoveAll(path) }
}

func writeSSTable(t *testing.T, config *ControllerConfig, id int, data map[string][]byte) func() {
	dataFile, err := os.Create(filepath.Join(config.Dir, strconv.Itoa(id)+".data"))
	require.NoError(t, err)

	indexFile, err := os.Create(filepath.Join(config.Dir, strconv.Itoa(id)+".index"))
	require.NoError(t, err)

	store, err := storage.NewStoreWithData(data)
	require.NoError(t, err)
	require.NoError(t, sstable.NewWriter(dataFile, indexFile, store).Write())

	return func() {
		_ = os.Remove(dataFile.Name())
		_ = os.Remove(indexFile.Name())
	}
}
