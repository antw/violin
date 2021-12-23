package sstable

type storeIterator struct {
	store SerializableStore
	c     chan *KeyValue
}

func (s *storeIterator) Next() (*KeyValue, bool) {
	s.start()

	kv, ok := <-s.c
	return kv, ok
}

func (s *storeIterator) start() {
	if s.c != nil {
		return
	}

	s.c = make(chan *KeyValue, 1)

	go func() {
		s.store.Ascend(func(key string, value []byte) bool {
			s.c <- &KeyValue{Key: key, Value: value}
			return true
		})
		close(s.c)
	}()
}
