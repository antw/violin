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
)

type controller struct {
	activeStore    *storage.Store
	readableStores []storage.ReadableStore
	config         ControllerConfig

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

//var _ storage.ReadableStore = &controller{}
var _ storage.WritableStore = &controller{}

// NewController creates a new controller, which encapsulates zero or more readable stores from disk
// with an in-memory store for updates. The controller is responsible for managing the active store
// and will periodically write its data to an SSTable, replacing the active store with a new, empty
// one.
//
// This takes ownership of the given readableStores, and these stores should not be interacted with
// again by the caller.
func NewController(readableStores []storage.ReadableStore, config ControllerConfig) *controller {
	if config.FlushBytes == 0 {
		config.FlushBytes = 1024 * 1024 * 64
	}

	if config.FlushFrequency == 0 {
		config.FlushFrequency = 5 * time.Second
	}

	return &controller{
		activeStore:    storage.NewStore(),
		readableStores: readableStores,
		config:         config,
		close:          make(chan struct{}),
	}
}

func LoadController(config ControllerConfig) (*controller, error) {
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

	return NewController(stores, config), nil
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

	return c.flushActiveStore()
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
		return val, nil
	} else if !errors.Is(err, storage.ErrNoSuchKey) {
		return nil, err
	}

	for i := len(c.readableStores) - 1; i >= 0; i-- {
		if val, err := c.readableStores[i].Get(key); err == nil {
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
