package sstable

import (
	"container/heap"
)

// multiStore implements SerializableStore across multiple SerializableStores so that all their
// key/value pairs can be iterated over in lexographic order.
type aggregator struct {
	stores []SerializableStore
}

// Ascend calls iterator for each key/value pair in each store, in lexographic order.
func (a *aggregator) Ascend(iterator func(key string, value []byte) bool) {
	pq := &mergeHeap{}
	iterators := make([]*storeIterator, len(a.stores))

	for i, store := range a.stores {
		iterators[i] = &storeIterator{store: store}

		if item, ok := iterators[i].Next(); ok {
			heap.Push(pq, &mergeItem{kv: item, iterIndex: i})
		}
	}

	for pq.Len() > 0 {
		item := heap.Pop(pq).(*mergeItem)
		conflict := false

		// Check that no later iterator has a conflicting key. This is surely not the optimal way
		// of doing this... perhaps a separate priority queue data structure could be used, wrapping
		// around mergeHeap, that would handle de-duplication when adding new items?
		for _, other := range *pq {
			if other.kv.Key == item.kv.Key && other.iterIndex > item.iterIndex {
				conflict = true
				break
			}
		}

		if !conflict && !iterator(item.kv.Key, item.kv.Value) {
			return
		}

		if nextItem, ok := iterators[item.iterIndex].Next(); ok {
			heap.Push(pq, &mergeItem{kv: nextItem, iterIndex: item.iterIndex})
		}
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
	return h[i].kv.Key < h[j].kv.Key
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
