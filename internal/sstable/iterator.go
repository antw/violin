package sstable

import (
	"container/heap"
	"strings"

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
	s  chan struct{}
}

func NewIterator(it CurriedIterable) *Iterator {
	return &Iterator{fn: it}
}

func (it *Iterator) Next() (*KeyValue, bool) {
	it.start()

	kv, ok := <-it.c
	return kv, ok
}

// Release releases the iterators resources, stopping the goroutine which is responsible for
// fetching new values from the store. This is only required when providing an iterator function
// that may prematurely terminate iteration.
func (it *Iterator) Release() {
	select {
	case it.s <- struct{}{}:
	default:
	}
}

func (it *Iterator) start() {
	if it.c != nil {
		return
	}

	it.c = make(chan *KeyValue, 1)
	it.s = make(chan struct{}, 1)

	go func() {
		it.fn(func(key string, value []byte) bool {
			select {
			case it.c <- &KeyValue{Key: key, Value: value}:
				return true
			case <-it.s:
				// Stop iterator and end the goroutine.
				return false
			}
		})
		close(it.c)
	}()
}

// -------------------------------------------------------------------------------------------------

// AggregatedIterator provides an Iterator-like interface over a collection of iterators.
type AggregatedIterator struct {
	iters []*Iterator
	pq    *mergeHeap
}

func NewAggregatedIterator(fns []CurriedIterable) *AggregatedIterator {
	pq := &mergeHeap{}
	iterators := make([]*Iterator, len(fns))

	for i, fn := range fns {
		iterators[i] = NewIterator(fn)

		if item, ok := iterators[i].Next(); ok {
			heap.Push(pq, &mergeItem{kv: item, iterIndex: i})
		}
	}

	return &AggregatedIterator{iters: iterators, pq: pq}
}

// Next calls iterator for each key/value pair in each store, in lexographic order.
func (ai *AggregatedIterator) Next() (*KeyValue, bool) {
	if ai.pq.Len() == 0 {
		return nil, false
	}

	for ai.pq.Len() > 0 {
		item := heap.Pop(ai.pq).(*mergeItem)
		conflict := false

		// Check that no later iterator has a conflicting key. If a conflict is found it will always
		// be the next element in the heap.
		if next := ai.pq.Peek(); next != nil && next.kv.Key == item.kv.Key {
			conflict = true
		}

		if nextItem, ok := ai.iters[item.iterIndex].Next(); ok {
			heap.Push(ai.pq, &mergeItem{kv: nextItem, iterIndex: item.iterIndex})
		}

		if conflict {
			continue
		}

		return item.kv, true
	}

	return nil, false
}

func (ai *AggregatedIterator) Release() {
	for _, it := range ai.iters {
		it.Release()
	}
}

// -------------------------------------------------------------------------------------------------

type mergeItem struct {
	kv        *KeyValue
	iterIndex int
}

// itemHeap implements heap.Interface.
type mergeHeap []*mergeItem

var _ heap.Interface = (*mergeHeap)(nil)

func (h mergeHeap) Len() int {
	return len(h)
}

func (h mergeHeap) Less(i, j int) bool {
	switch strings.Compare(h[i].kv.Key, h[j].kv.Key) {
	case -1:
		return true
	case 1:
		return false
	default:
		// Two kvs with the same key are ordered so that the one from the first iterator appears
		// first.
		return h[i].iterIndex < h[j].iterIndex
	}
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(*mergeItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]

	return x
}

func (h *mergeHeap) Peek() *mergeItem {
	hp := *h

	if len(hp) == 0 {
		return nil
	}

	return hp[0]
}
