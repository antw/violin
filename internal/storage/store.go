package storage

import (
	"sync"
)

type Store struct {
	data sync.Map
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
