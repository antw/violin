package server

import (
	"errors"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antw/violin/internal/sstable"
	"github.com/antw/violin/internal/storage"
	"github.com/antw/violin/internal/wal"
)

var (
	ErrDirNotEmpty = errors.New("controller: cannot create new controller in non-empty directory")
)

type controller struct {
	activeStore    *storage.Store
	readableStores []storage.ReadableStore
	config         ControllerConfig

	wal *wal.WAL

	// Contains a *rough* estimate of the number of bytes stored in the active store.
	estimatedSize int64

	// A mutex which is write-locked whenever changing the active or readable stores. A read lock is
	// used in all other situations, and write-locking is left to the implementation of the active
	// store.
	mu sync.RWMutex

	// A message is sent when the controller is shut down.
	close chan struct{}
}

type ControllerConfig struct {
	Dir string

	// FlushBytes sets at what size the active store is flushed to an SSTable.
	FlushBytes int64

	// How often to check the active store size to attempt a flush.
	FlushFrequency time.Duration
}

func (c *ControllerConfig) WALPath() string {
	return filepath.Join(c.Dir, "wal.log")
}

var _ storage.ReadableStore = &controller{}
var _ storage.WritableStore = &controller{}

// NewController creates a new controller, which encapsulates zero or more readable stores from disk
// with an in-memory store for updates. The controller is responsible for managing the active store
// and will periodically write its data to an SSTable, replacing the active store with a new, empty
// one.
//
// NewController will error if the given config.Dir contains any files.
//
// This takes ownership of the given readableStores, and these stores should not be interacted with
// again by the caller.
func NewController(
	readableStores []storage.ReadableStore,
	config ControllerConfig,
) (*controller, error) {
	if filelist, err := ioutil.ReadDir(config.Dir); err != nil {
		return nil, err
	} else if len(filelist) > 0 {
		return nil, ErrDirNotEmpty
	}

	wal, err := wal.NewWithPath(config.WALPath())
	if err != nil {
		return nil, err
	}

	return buildController(storage.NewStore(), readableStores, config, wal)
}

// OpenController creates a new controller using the files in the config.Dir.
//
// This includes reading any existing WAL and applying them to the active store, as well as loading
// the indicies for any SSTables.
func OpenController(config ControllerConfig) (*controller, error) {
	stores := make([]storage.ReadableStore, 0)

	for _, path := range listIndexFiles(config.Dir) {
		sst, err := sstable.OpenSSTable(
			filepath.Join(config.Dir, path[:len(path)-6]+".data"),
			filepath.Join(config.Dir, path),
		)
		if err != nil {
			return nil, err
		}

		stores = append(stores, sst)
	}

	wlog, store, err := walAndStore(config.WALPath())
	if err != nil {
		return nil, err
	}

	return buildController(store, stores, config, wlog)
}

// buildController creates a new controller instance with the given stores, config, and WAL.
func buildController(
	activeStore *storage.Store,
	readableStores []storage.ReadableStore,
	config ControllerConfig,
	wlog *wal.WAL,
) (*controller, error) {
	os.MkdirAll(config.Dir, 0700)

	if config.FlushBytes == 0 {
		config.FlushBytes = 1024 * 1024 * 64
	}

	if config.FlushFrequency == 0 {
		config.FlushFrequency = 5 * time.Second
	}

	return &controller{
		activeStore:    activeStore,
		readableStores: readableStores,
		config:         config,
		wal:            wlog,
		close:          make(chan struct{}),
	}, nil
}

// Returns a sorted list of all index file names in the given directory.
func listIndexFiles(dir string) []string {
	var paths []string

	items, _ := ioutil.ReadDir(dir)
	for _, item := range items {
		if !item.IsDir() && strings.HasSuffix(item.Name(), ".index") {
			paths = append(paths, item.Name())
		}
	}
	sort.Strings(paths)

	return paths
}

func (c *controller) Start() {
	ticker := time.NewTicker(c.config.FlushFrequency)
	go func() {
		for {
			select {
			case <-ticker.C:
				if atomic.LoadInt64(&c.estimatedSize) >= c.config.FlushBytes {
					if err := c.flushActiveStore(); err != nil {
						log.Fatal(err)
					}
				}
			case <-c.close:
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *controller) Close() error {
	c.close <- struct{}{}

	if err := c.wal.Close(); err != nil {
		return err
	}

	return c.flushActiveStore()
}

// Ascend calls the iterator for each key/value pair in the store, until the iterator returns false.
// Tombstoned keys are skipped.
func (c *controller) Ascend(it storage.Iterator) {
	c.iterateAggregated(
		func(store storage.ReadableStore) sstable.CurriedIterable {
			return store.Ascend
		},
		it,
	)
}

// AscendRange calls the iterator for every value in the tree within the range
// [greaterOrEqual, lessThan), until iterator returns false. Tombstoned keys are not included.
func (c *controller) AscendRange(
	greaterOrEqual string,
	lessThan string,
	it storage.Iterator,
) {
	c.iterateAggregated(
		func(store storage.ReadableStore) sstable.CurriedIterable {
			return func(innerIt storage.Iterator) {
				store.AscendRange(greaterOrEqual, lessThan, innerIt)
			}
		},
		it,
	)
}

func (c *controller) Delete(key string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	c.updateEstimatedSize(key, 0)

	if err := c.activeStore.Delete(key); err != nil {
		return err
	}

	return nil
}

func (c *controller) Get(key string) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if val, err := c.activeStore.Get(key); err == nil {
		if val == nil {
			return nil, storage.ErrNoSuchKey
		}
		return val, nil
	} else if !errors.Is(err, storage.ErrNoSuchKey) {
		return nil, err
	}

	for i := len(c.readableStores) - 1; i >= 0; i-- {
		if val, err := c.readableStores[i].Get(key); err == nil {
			if val == nil {
				return nil, storage.ErrNoSuchKey
			}
			return val, nil
		} else if !errors.Is(err, storage.ErrNoSuchKey) {
			return nil, err
		}
	}

	return nil, storage.ErrNoSuchKey
}

func (c *controller) Set(key string, value []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	c.updateEstimatedSize(key, int64(len(value)))

	if err := c.activeStore.Set(key, value); err != nil {
		return err
	}

	return nil
}

// allStores returns a list of all readable stores, including the active store.
func (c *controller) allStores() []storage.ReadableStore {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stores := make([]storage.ReadableStore, len(c.readableStores)+1)
	for i, store := range c.readableStores {
		stores[i] = store
	}

	stores[len(c.readableStores)] = c.activeStore

	return stores
}

// iterateAggregated allows iterating over key-values pairs in all stores in the controller. Each
// key is yielded only once, with older values ignored.
//
// The `getter` function is called for each readable store in the controller in order to iterate
// through the values in each controller. The `it` iterator is called for each key-value pair.
//
// For example:
//
//     c.iterateAggregated(
//         func(store storage.ReadableStore) sstable.CurriedIterable {
//             return store.Ascend
//         },
//         func(key string, value []byte) bool {
//             // Do something with the key-value pair.
//             return true
//         },
//     )
func (c *controller) iterateAggregated(
	getter func(storage.ReadableStore) sstable.CurriedIterable,
	it storage.Iterator,
) {
	stores := c.allStores()
	fns := make([]sstable.CurriedIterable, len(stores))

	for i, store := range stores {
		fns[i] = func(store storage.ReadableStore) sstable.CurriedIterable {
			return getter(store)
		}(store)
	}

	iterator := sstable.NewAggregatedIterator(fns)
	defer iterator.Release()

	for got, ok := iterator.Next(); ok; got, ok = iterator.Next() {
		if got.GetValue() != nil && !it(got.GetKey(), got.GetValue()) {
			break
		}
	}
}

// updateEstimatedSize updates the estimated size of the active store based on an update of the
// given key and the size of the new value.
func (c *controller) updateEstimatedSize(key string, newSize int64) {
	for i := 0; i < 5; i++ {
		oldSize, err := c.sizeOf(key)
		if err != nil {
			log.Printf("error getting size of key %s: %v\n", key, err)
			return
		}

		if keyLen := int64(len(key)); oldSize != keyLen {
			// Updating key size would not increase storage size.
			oldSize -= keyLen
		}

		currentSize := atomic.LoadInt64(&c.estimatedSize)
		updatedSize := currentSize - oldSize + newSize

		if updatedSize < 0 {
			updatedSize = 0
		}

		if atomic.CompareAndSwapInt64(&c.estimatedSize, currentSize, updatedSize) {
			return
		}

		// Compare and swap failed, so we'll try again to update the estimated size after a short
		// wait.
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(2*i+2)))
	}

	log.Printf("failed to update estimated size of key %s\n", key)
}

// sizeOf returns the size of the given key-value pair in the active store.
func (c *controller) sizeOf(key string) (int64, error) {
	value, err := c.activeStore.Get(key)

	if err != nil || value == nil {
		if errors.Is(err, storage.ErrNoSuchKey) || value == nil {
			return 0, nil
		}
		return 0, err
	}

	return int64(len(value)) + int64(len(key)), nil
}

// flushActiveStore writes the active store to an SSTable and replaces it with a new, empty store.
func (c *controller) flushActiveStore() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.activeStore.Len() == 0 {
		return nil
	}

	timestamp := strconv.FormatUint(uint64(time.Now().UnixMilli()), 10)

	dataFile, err := os.Create(filepath.Join(c.config.Dir, timestamp+".data"))
	if err != nil {
		return err
	}

	indexFile, err := os.Create(filepath.Join(c.config.Dir, timestamp+".index"))
	if err != nil {
		return err
	}

	if err := sstable.NewWriter(dataFile, indexFile, c.activeStore).Write(); err != nil {
		return err
	}

	dataFile, err = os.Open(dataFile.Name())
	if err != nil {
		return err
	}

	indexFile, err = os.Open(indexFile.Name())
	if err != nil {
		return err
	}

	table, err := sstable.NewSSTable(dataFile, indexFile)
	if err != nil {
		return err
	}

	// Add the new sstable to the list of readable stores.
	c.readableStores = append(c.readableStores, table)

	c.activeStore = storage.NewStore()
	atomic.StoreInt64(&c.estimatedSize, 0)

	log.Printf("flushed active store to %s and %s", dataFile.Name(), indexFile.Name())

	return nil
}

// openWAL takes a path to a WAL file and returns the WAL object, and a store containing any
// updates that were written to the WAL.
func openWAL(path string) (*wal.WAL, *storage.Store, error) {
	store := storage.NewStore()

	wlog, err := wal.OpenWithPath(path, func(rec *wal.Record) {
		if up := rec.GetUpsert(); up != nil {
			store.Set(up.GetKey(), up.GetValue())
		} else if del := rec.GetDelete(); del != nil {
			store.Delete(del.GetKey())
		}
	})
	if err != nil {
		return nil, nil, err
	}

	return wlog, store, nil
}

// walAndStore takes a path to a possible WAL file and returns the WAL and store representing the
// entried contained. If no file exists, a new WAL and store is created and returned.
func walAndStore(path string) (*wal.WAL, *storage.Store, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		// No WAL exists, so create a new one.
		log, err := wal.NewWithPath(path)
		if err != nil {
			return nil, nil, err
		}

		return log, storage.NewStore(), nil
	} else if err != nil {
		return nil, nil, err
	}

	return openWAL(path)
}
