package storage

import (
	"strings"
	"sync"
)

type Store struct {
	data sync.Map
}

// Pair is returned by Between and contains a key/value pair.
type Pair struct {
	Key   string
	Value []byte
}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) Get(key string) (value []byte, ok bool) {
	val, ok := s.data.Load(key)
	if !ok {
		return nil, false
	}

	return val.([]byte), true
}

func (s *Store) Set(key string, value []byte) error {
	s.data.Store(key, value)
	return nil
}

// Between returns all the keys and values whose keys are lexographically between the given start
// and end key.
//
// This is currently extremely inefficient, iterating through all keys in the store.
func (s *Store) Between(start, end string) []Pair {
	var pairs []Pair

	s.data.Range(func(key, value interface{}) bool {
		stringKey := key.(string)

		if strings.Compare(stringKey, start) >= 0 && strings.Compare(stringKey, end) <= 0 {
			pairs = append(pairs, Pair{stringKey, value.([]byte)})
		}

		return true
	})

	return pairs
}
