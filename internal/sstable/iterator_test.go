package sstable

import (
	"container/heap"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/antw/violin/internal/storage"
)

func TestIterator_WithAscend(t *testing.T) {
	store, err := storage.NewStoreWithData(map[string][]byte{
		"foo": []byte("1"),
		"bar": []byte("2"),
	})
	require.NoError(t, err)

	iterator := NewIterator(store.Ascend)

	kv, ok := iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "bar", Value: []byte("2")}, kv)

	kv, ok = iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "foo", Value: []byte("1")}, kv)

	kv, ok = iterator.Next()
	require.False(t, ok)
	require.Nil(t, kv)
}

func TestIterator_WithAscend_EarlyExit(t *testing.T) {
	store, err := storage.NewStoreWithData(map[string][]byte{
		"foo": []byte("1"),
		"bar": []byte("2"),
		"baz": []byte("3"),
	})
	require.NoError(t, err)

	iterator := NewIterator(func(it storage.Iterator) {
		store.Ascend(func(key string, value []byte) bool {
			if !it(key, value) {
				return false
			}

			return key < "baz"
		})
	})
	defer iterator.Release()

	kv, ok := iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "bar", Value: []byte("2")}, kv)

	kv, ok = iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "baz", Value: []byte("3")}, kv)

	kv, ok = iterator.Next()
	require.False(t, ok)
	require.Nil(t, kv)
}

func TestIterator_WithAscendBetween(t *testing.T) {
	store, err := storage.NewStoreWithData(map[string][]byte{
		"foo": []byte("1"),
		"bar": []byte("2"),
		"baz": []byte("3"),
	})
	require.NoError(t, err)

	iterator := NewIterator(func(it storage.Iterator) {
		store.AscendRange("bar", "foo", it)
	})

	kv, ok := iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "bar", Value: []byte("2")}, kv)

	kv, ok = iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "baz", Value: []byte("3")}, kv)

	kv, ok = iterator.Next()
	require.False(t, ok)
	require.Nil(t, kv)
}

// -------------------------------------------------------------------------------------------------

func TestAggregatedIterator_Empty(t *testing.T) {
	agg := NewAggregatedIterator(createAscendFns())

	_, ok := agg.Next()
	require.False(t, ok)

	_, ok = agg.Next()
	require.False(t, ok)
}

func TestAggregatedIterator_OneStore(t *testing.T) {
	expects := []struct {
		k string
		v []byte
	}{
		{"a", []byte("one")},
		{"b", []byte("two")},
	}

	store := storage.NewStore()

	for _, kv := range expects {
		err := store.Set(kv.k, kv.v)
		require.NoError(t, err)
	}

	agg := NewAggregatedIterator(createAscendFns(store))

	for _, expected := range expects {
		val, ok := agg.Next()

		require.True(t, ok)

		require.Equal(t, expected.k, val.GetKey())
		require.Equal(t, string(expected.v), string(val.GetValue()))
	}

	val, ok := agg.Next()
	require.False(t, ok)
	require.Nil(t, val)
}

func TestAggregatedIterator_TwoStores(t *testing.T) {
	fmt.Println("-- two stores")
	older, err := storage.NewStoreWithData(map[string][]byte{
		"a": []byte("one"),
		"c": []byte("one"),
	})
	require.NoError(t, err)

	newer, err := storage.NewStoreWithData(map[string][]byte{
		"b": []byte("two"),
	})
	require.NoError(t, err)

	agg := NewAggregatedIterator(createAscendFns(older, newer))
	var keys []string

	for got, ok := agg.Next(); ok; got, ok = agg.Next() {
		keys = append(keys, got.GetKey())
	}

	// Asserts that non-conflicting keys after the conflict are still yielded.
	require.Equal(t, 3, len(keys))
	require.Equal(t, []string{"a", "b", "c"}, keys)

	got, ok := agg.Next()
	require.False(t, ok)
	require.Nil(t, got)
}

func TestAggregatedIterator_TwoConflictingStores(t *testing.T) {
	older, err := storage.NewStoreWithData(map[string][]byte{
		"a": []byte("one"),
		"c": []byte("one"),
		"e": []byte("one"),
	})
	require.NoError(t, err)

	newer, err := storage.NewStoreWithData(map[string][]byte{
		"b": []byte("two"),
		"c": []byte("two"),
		"d": []byte("two"),
	})
	require.NoError(t, err)

	agg := NewAggregatedIterator(createAscendFns(older, newer))

	var keys []string
	var vals []string

	for got, ok := agg.Next(); ok; got, ok = agg.Next() {
		keys = append(keys, got.GetKey())
		vals = append(vals, string(got.GetValue()))
	}

	// Asserts that non-conflicting keys after the conflict are still yielded.
	require.Equal(t, 5, len(keys))
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
	require.Equal(t, []string{"one", "two", "two", "two", "one"}, vals)

	got, ok := agg.Next()
	require.False(t, ok)
	require.Nil(t, got)
}

func TestAggregatedIterator_TwoStoresNewDeletedKey(t *testing.T) {
	older, err := storage.NewStoreWithData(map[string][]byte{
		"a": []byte("one"),
		"c": []byte("one"),
	})
	require.NoError(t, err)

	newer, err := storage.NewStoreWithData(map[string][]byte{
		"b": []byte("two"),
		"c": []byte("two"),
	})
	require.NoError(t, err)

	err = newer.Delete("c")
	require.NoError(t, err)

	agg := NewAggregatedIterator(createAscendFns(older, newer))

	var keys []string
	var vals []string

	for got, ok := agg.Next(); ok; got, ok = agg.Next() {
		keys = append(keys, got.GetKey())
		vals = append(vals, string(got.GetValue()))
	}

	require.Equal(t, 3, len(keys))
	require.Equal(t, []string{"a", "b", "c"}, keys)
	require.Equal(t, []string{"one", "two", ""}, vals)

	got, ok := agg.Next()
	require.False(t, ok)
	require.Nil(t, got)
}

func TestAggregatedIterator_TwoStoresOldDeletedKey(t *testing.T) {
	older, err := storage.NewStoreWithData(map[string][]byte{
		"a": []byte("one"),
		"c": []byte("one"),
	})
	require.NoError(t, err)

	newer, err := storage.NewStoreWithData(map[string][]byte{
		"b": []byte("two"),
		"c": []byte("two"),
	})
	require.NoError(t, err)

	err = older.Delete("c")
	require.NoError(t, err)

	agg := NewAggregatedIterator(createAscendFns(older, newer))

	var keys []string
	var vals []string

	for got, ok := agg.Next(); ok; got, ok = agg.Next() {
		keys = append(keys, got.GetKey())
		vals = append(vals, string(got.GetValue()))
	}

	require.Equal(t, 3, len(keys))
	require.Equal(t, []string{"a", "b", "c"}, keys)
	require.Equal(t, []string{"one", "two", "two"}, vals)

	got, ok := agg.Next()
	require.False(t, ok)
	require.Nil(t, got)
}

func TestAggregatedIterator_WithAscend_EarlyExit(t *testing.T) {
	store, err := storage.NewStoreWithData(map[string][]byte{
		"foo": []byte("1"),
		"bar": []byte("2"),
		"baz": []byte("3"),
	})
	require.NoError(t, err)

	inner := func(it storage.Iterator) {
		store.Ascend(func(key string, value []byte) bool {
			if !it(key, value) {
				return false
			}

			return key < "baz"
		})
	}
	iterator := NewAggregatedIterator([]CurriedIterable{inner})
	defer iterator.Release()

	kv, ok := iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "bar", Value: []byte("2")}, kv)

	kv, ok = iterator.Next()
	require.True(t, ok)
	require.Equal(t, &KeyValue{Key: "baz", Value: []byte("3")}, kv)

	kv, ok = iterator.Next()
	require.False(t, ok)
	require.Nil(t, kv)
}

//--------------------------------------------------------------------------------------------------

func TestMergeHeapPushPop(t *testing.T) {
	list := mergeHeap{}

	a := mergeItem{kv: &KeyValue{Key: "a"}, iterIndex: 0}
	b := mergeItem{kv: &KeyValue{Key: "b"}, iterIndex: 1}
	c := mergeItem{kv: &KeyValue{Key: "c"}, iterIndex: 2}

	heap.Push(&list, &b)
	heap.Push(&list, &c)
	heap.Push(&list, &a)

	require.Equal(t, a, *heap.Pop(&list).(*mergeItem))
	require.Equal(t, b, *heap.Pop(&list).(*mergeItem))
	require.Equal(t, c, *heap.Pop(&list).(*mergeItem))
}

func TestMergeHeapLen(t *testing.T) {
	list := mergeHeap{}

	require.Equal(t, list.Len(), 0)
	require.Equal(t, len(list), 0)

	heap.Push(&list, &mergeItem{kv: &KeyValue{Key: "a"}, iterIndex: 0})
	heap.Push(&list, &mergeItem{kv: &KeyValue{Key: "b"}, iterIndex: 1})
	require.Equal(t, list.Len(), 2)

	heap.Pop(&list)
	require.Equal(t, list.Len(), 1)

	heap.Pop(&list)
	require.Equal(t, list.Len(), 0)
}

func TestMergeHeapLess(t *testing.T) {
	a := mergeItem{kv: &KeyValue{Key: "a"}, iterIndex: 0}
	b := mergeItem{kv: &KeyValue{Key: "b"}, iterIndex: 1}

	mh := mergeHeap([]*mergeItem{&a, &b})

	// Different key.
	require.True(t, mh.Less(0, 1))
	require.False(t, mh.Less(1, 0))

	// Same key.
	b.kv.Key = "a"
	require.True(t, mh.Less(0, 1))
	require.False(t, mh.Less(1, 0))
}

// -------------------------------------------------------------------------------------------------

func createAscendFns(stores ...*storage.Store) []CurriedIterable {
	fns := make([]CurriedIterable, len(stores))

	for i := range stores {
		idx := i
		fns[idx] = func(it storage.Iterator) {
			stores[idx].Ascend(it)
		}
	}

	return fns
}
