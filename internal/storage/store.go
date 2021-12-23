package storage

import (
	"sync"

	"github.com/google/btree"
)

type Store struct {
	data    sync.Map
	index   btree.BTree
	indexMu sync.RWMutex
}

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

func (s *Store) Get(key string) (value []byte, ok bool) {
	val, ok := s.data.Load(key)
	if !ok {
		return nil, false
	}

	return val.([]byte), true
}

func (s *Store) Set(key string, value []byte) error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	s.data.Store(key, value)
	s.index.ReplaceOrInsert(indexKey(key))

	return nil
}

// Ascend calls the iterator for each key/value pair in the store, until the iterator returns false.
func (s *Store) Ascend(iterator func(key string, value []byte) bool) {
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
func (s *Store) AscendRange(
	greaterOrEqual string,
	lessThan string,
	iterator func(key string, value []byte) bool,
) {
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

// indexKey implements btree.Item for strings.
type indexKey string

func (ik indexKey) Less(than btree.Item) bool {
	return ik < than.(indexKey)
}
