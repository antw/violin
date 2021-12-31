package sstable

import (
	"github.com/antw/violin/internal/storage"
)

// CurriedIterable represents a function that can be called with an iterator, yielding each value in
// a store while the iterator returns true.
//
// For example:
//
//   start := "foo"
//   end := "between"
//   var it CurriedIterable = func(iterator) { store.AscendRange(start, end, iterator) }
type CurriedIterable func(storage.Iterator)

// Iterator takes any CurriedIterable and returns an iterator over each key-value pair yielded by
// the iterable.
type Iterator struct {
	fn CurriedIterable
	c  chan *KeyValue
}

func NewIterator(it CurriedIterable) *Iterator {
	return &Iterator{fn: it}
}

func (it *Iterator) Next() (*KeyValue, bool) {
	it.start()

	kv, ok := <-it.c
	return kv, ok
}

func (it *Iterator) start() {
	if it.c != nil {
		return
	}

	it.c = make(chan *KeyValue, 1)

	go func() {
		it.fn(func(key string, value []byte) bool {
			it.c <- &KeyValue{Key: key, Value: value}
			return true
		})
		close(it.c)
	}()
}
