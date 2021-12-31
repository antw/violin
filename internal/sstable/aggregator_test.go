package sstable

import (
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

func TestAggregatorWithNewerDeletedKey(t *testing.T) {
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

	agg := aggregator{stores: []storage.SerializableStore{older, newer}}

	var keys []string
	var vals []string

	agg.Ascend(func(k string, v []byte) bool {
		keys = append(keys, k)
		vals = append(vals, string(v))
		return true
	})

	// Asserts that non-conflicting keys after the conflict are still yielded.
	require.Equal(t, 3, len(keys))
	require.Equal(t, []string{"a", "b", "c"}, keys)
	require.Equal(t, []string{"one", "two", ""}, vals)
}

func TestAggregatorWithOlderDeletedKey(t *testing.T) {
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

	agg := aggregator{stores: []storage.SerializableStore{older, newer}}

	var keys []string
	var vals []string

	agg.Ascend(func(k string, v []byte) bool {
		keys = append(keys, k)
		vals = append(vals, string(v))
		return true
	})

	// Asserts that non-conflicting keys after the conflict are still yielded.
	require.Equal(t, 3, len(keys))
	require.Equal(t, []string{"a", "b", "c"}, keys)
	require.Equal(t, []string{"one", "two", "two"}, vals)
}
