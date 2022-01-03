package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func TestIterator_Next(t *testing.T) {
	w, teardown := createNewWAL(t, "wal_iterator_next")
	defer teardown()

	err := w.Upsert(1, "foo", []byte("foo value"))
	require.NoError(t, err)

	err = w.Delete(2, "bar")
	require.NoError(t, err)

	err = w.Upsert(3, "bar", []byte("bar value"))
	require.NoError(t, err)

	require.NoError(t, w.Close())

	it := createIterator(t, w.file.Name())

	rec, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), rec.GetTxid())
	require.Equal(t, "foo", rec.GetUpsert().GetKey())
	require.Equal(t, "foo value", string(rec.GetUpsert().GetValue()))

	rec, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(2), rec.GetTxid())
	require.Equal(t, "bar", rec.GetDelete().GetKey())

	rec, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(3), rec.GetTxid())
	require.Equal(t, "bar", rec.GetUpsert().GetKey())
	require.Equal(t, "bar value", string(rec.GetUpsert().GetValue()))

	_, err = it.Next()
	require.ErrorIs(t, iterator.Done, err)
}

func TestIterator_SkipTo(t *testing.T) {
	w, teardown := createNewWAL(t, "wal_iterator_peek")
	defer teardown()

	err := w.Upsert(1, "foo", []byte("foo value"))
	require.NoError(t, err)

	err = w.Delete(2, "bar")
	require.NoError(t, err)

	err = w.Upsert(3, "bar", []byte("bar value"))
	require.NoError(t, err)

	require.NoError(t, w.Close())

	it := createIterator(t, w.file.Name())

	require.NoError(t, it.SkipTo(2))

	rec, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(2), rec.GetTxid())
	require.Equal(t, "bar", rec.GetDelete().GetKey())

	// Can't skip backwards.
	err = it.SkipTo(1)
	require.ErrorIs(t, ErrInvalidSkip, err)

	// Can't skip to missing ID.
	err = it.SkipTo(4)
	require.ErrorIs(t, ErrNoSuchTxid, err)
}

// -------------------------------------------------------------------------------------------------

func createIterator(t *testing.T, path string) *Iterator {
	f, err := os.Open(path)
	require.NoError(t, err)

	return NewIterator(f)
}
