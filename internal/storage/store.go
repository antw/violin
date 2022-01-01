package storage

import (
	"errors"
	"sync"

	"github.com/google/btree"
)

var (
	// ErrNoSuchKey is used when trying to fetch a key which doesn't exist.
	ErrNoSuchKey = errors.New("no such key")
)

type Store struct {
	data    sync.Map
	index   btree.BTree
	indexMu sync.RWMutex
}

// Iterator is used in Ascend and AscendRange. Each key-value pair is yielded to the function. The
// function may return false to end iteration.
type Iterator func(key string, value []byte) bool

type ReadableStore interface {
	SerializableStore
	AscendRange(greaterOrEqual string, lessThan string, iterator Iterator)

	// Get fetches the value of a key. Get may return an ErrNoSuchKey error if it has never been
	// seen, or a value of nil if it has been set and later deleted.
	Get(key string) (value []byte, err error)
}

// SerializableStore is an interface which describes any kind of key/value store whose values may
// be iterated and yielded to a function in lexographical order.
type SerializableStore interface {
	Ascend(iterator Iterator)
}

type WritableStore interface {
	Set(key string, value []byte) error
	Delete(key string) error
}

var _ ReadableStore = (*Store)(nil)
var _ WritableStore = (*Store)(nil)

// Pair is returned by Between and contains a key/value pair.
type Pair struct {
	Key   string
	Value []byte
}

func NewStore() *Store {
	return &Store{index: *btree.New(2)}
}

// NewStoreWithData creates a new store with the given data. Useful for testing.
func NewStoreWithData(data map[string][]byte) (*Store, error) {
	s := NewStore()

	for k, v := range data {
		err := s.Set(k, v)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *Store) Get(key string) (value []byte, err error) {
	val, ok := s.data.Load(key)
	if !ok {
		return nil, ErrNoSuchKey
	}

	return val.([]byte), nil
}

func (s *Store) Set(key string, value []byte) error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	s.data.Store(key, value)
	s.index.ReplaceOrInsert(indexKey(key))

	return nil
}

// Ascend calls the iterator for each key/value pair in the store, until the iterator returns false.
func (s *Store) Ascend(iterator Iterator) {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()

	s.index.Ascend(func(item btree.Item) bool {
		key := string(item.(indexKey))
		value, ok := s.data.Load(key)

		if !ok {
			return true
		}

		return iterator(key, value.([]byte))
	})
}

// AscendRange calls the iterator for every value in the tree within the range
// [greaterOrEqual, lessThan), until iterator returns false.
func (s *Store) AscendRange(greaterOrEqual string, lessThan string, iterator Iterator) {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()

	s.index.AscendRange(indexKey(greaterOrEqual), indexKey(lessThan), func(item btree.Item) bool {
		key := string(item.(indexKey))
		value, ok := s.data.Load(key)

		if !ok {
			return true
		}

		return iterator(key, value.([]byte))
	})
}

// Between returns all the keys and values whose keys are lexographically between
// [greaterOrEqual, lessThan).
func (s *Store) Between(greaterOrEqual, lessThan string) []Pair {
	var pairs []Pair

	s.AscendRange(greaterOrEqual, lessThan, func(key string, value []byte) bool {
		pairs = append(pairs, Pair{Key: key, Value: value})
		return true
	})

	return pairs
}

// Delete removes a key from the store if it exists, setting its value to nil. A value of nil
// indicates that the key has been tombstoned.
func (s *Store) Delete(key string) error {
	return s.Set(key, nil)
}

// Len returns the number of keys in the store.
func (s *Store) Len() int {
	return s.index.Len()
}

// indexKey implements btree.Item for strings.
type indexKey string

func (ik indexKey) Less(than btree.Item) bool {
	return ik < than.(indexKey)
}
