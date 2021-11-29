package storage

import "sync"

type store struct {
	data sync.Map
}

func New() store {
	return store{}
}

func (s *store) Get(key string) (value []byte, ok bool) {
	val, ok := s.data.Load(key)
	if !ok {
		return nil, false
	}

	return val.([]byte), true
}

func (s *store) Set(key string, value []byte) {
	s.data.Store(key, value)
}
