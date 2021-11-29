package storage

import "sync"

type Store struct {
	data sync.Map
}

func New() Store {
	return Store{}
}

func (s *Store) Get(key string) (value []byte, ok bool) {
	val, ok := s.data.Load(key)
	if !ok {
		return nil, false
	}

	return val.([]byte), true
}

func (s *Store) Set(key string, value []byte) {
	s.data.Store(key, value)
}
