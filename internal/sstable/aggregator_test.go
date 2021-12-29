package sstable

import (
	"container/heap"
	"testing"

	"github.com/antw/violin/internal/storage"
	"github.com/stretchr/testify/require"
)

//--------------------------------------------------------------------------------------------------

func TestEmptyAggregator(t *testing.T) {
	agg := aggregator{}
	calls := 0

	agg.Ascend(func(k string, v []byte) bool {
		calls += 1
		return true
	})

	require.Zero(t, 0, calls)
}

func TestAggregatorWithOneStore(t *testing.T) {
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

	agg := aggregator{stores: []storage.SerializableStore{store}}
	calls := 0

	agg.Ascend(func(k string, v []byte) bool {
		expected := expects[calls]

		require.Equal(t, expected.k, k)
		require.Equal(t, expected.v, v)

		calls += 1
		return true
	})

	require.Equal(t, len(expects), calls)
}

func TestAggregatorWithTwoStores(t *testing.T) {
	older, err := storage.NewStoreWithData(map[string][]byte{
		"a": []byte("one"),
		"c": []byte("one"),
	})
	require.NoError(t, err)

	newer, err := storage.NewStoreWithData(map[string][]byte{
		"b": []byte("two"),
	})
	require.NoError(t, err)

	agg := aggregator{stores: []storage.SerializableStore{older, newer}}
	var keys []string

	agg.Ascend(func(k string, v []byte) bool {
		keys = append(keys, k)
		return true
	})

	// Asserts that non-conflicting keys after the conflict are still yielded.
	require.Equal(t, 3, len(keys))
	require.Equal(t, []string{"a", "b", "c"}, keys)
}

func TestAggregatorWithKeyConflict(t *testing.T) {
	older, err := storage.NewStoreWithData(map[string][]byte{
		"a": []byte("one"),
		"c": []byte("one"),
		"e": []byte("on"),
	})
	require.NoError(t, err)

	newer, err := storage.NewStoreWithData(map[string][]byte{
		"b": []byte("two"),
		"c": []byte("two"),
		"d": []byte("two"),
	})
	require.NoError(t, err)

	agg := aggregator{stores: []storage.SerializableStore{older, newer}}
	var keys []string

	agg.Ascend(func(k string, v []byte) bool {
		keys = append(keys, k)

		if k == "c" {
			require.Equal(t, []byte("two"), v)
		}

		return true
	})

	// Asserts that non-conflicting keys after the conflict are still yielded.
	require.Equal(t, 5, len(keys))
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
}

func TestAggregatorAscendEarlyExit(t *testing.T) {
	store, err := storage.NewStoreWithData(map[string][]byte{
		"a": []byte("one"),
		"b": []byte("two"),
		"c": []byte("three"),
	})

	require.NoError(t, err)

	agg := aggregator{stores: []storage.SerializableStore{store}}

	var seen []string
	calls := 0

	agg.Ascend(func(k string, v []byte) bool {
		seen = append(seen, k)

		calls += 1

		return k != "b"
	})

	require.Equal(t, 2, calls)
	require.Equal(t, "a", seen[0])
	require.Equal(t, "b", seen[1])
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
