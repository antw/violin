package sstable

import (
	"github.com/antw/violin/internal/storage"
)

// aggregator implements SerializableStore across multiple SerializableStores so that all their
// key/value pairs can be iterated over in lexographic order.
type aggregator struct {
	stores []storage.SerializableStore
}

var _ storage.SerializableStore = (*aggregator)(nil)

// Ascend calls iterator for each key/value pair in each store, in lexographic order.
func (a *aggregator) Ascend(iterator storage.Iterator) {
	fns := make([]CurriedIterable, len(a.stores))

	for i := range a.stores {
		idx := i
		fns[idx] = func(it storage.Iterator) { a.stores[idx].Ascend(it) }
	}

	it := NewAggregatedIterator(fns)
	defer it.Release()

	kv, ok := it.Next()
	for ok {
		if ret := iterator(kv.GetKey(), kv.GetValue()); !ret {
			break
		}
		kv, ok = it.Next()
	}
}
