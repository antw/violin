package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWAL_Delete(t *testing.T) {
	w, teardown := createNewWAL(t, "wal_upsert")
	defer teardown()

	err := w.Delete(1, "foo")
	require.NoError(t, err)

	require.NoError(t, w.Close())
}

func TestWAL_Upsert(t *testing.T) {
	w, teardown := createNewWAL(t, "wal_upsert")
	defer teardown()

	err := w.Upsert(1, "foo", []byte("foo value"))
	require.NoError(t, err)

	err = w.Upsert(2, "bar", []byte("bar value"))
	require.NoError(t, err)

	require.NoError(t, w.Close())
}

func TestWAL_WriteCausalityError(t *testing.T) {
	w, teardown := createNewWAL(t, "wal_upsert")
	defer teardown()

	err := w.Upsert(1, "foo", []byte("foo value"))
	require.NoError(t, err)

	// Can't conflict with the previous write.
	err = w.Upsert(1, "bar", []byte("bar value"))
	require.ErrorIs(t, ErrCausalityViolation, err)

	// Can't go backwards.
	err = w.Upsert(0, "bar", []byte("bar value"))
	require.ErrorIs(t, ErrCausalityViolation, err)

	// Can't skip forwards.
	err = w.Upsert(3, "bar", []byte("bar value"))
	require.ErrorIs(t, ErrCausalityViolation, err)

	err = w.Upsert(2, "bar", []byte("bar value"))
	require.NoError(t, err)
}

func TestLoadWAL_Yield(t *testing.T) {
	w, teardown := createNewWAL(t, "wal_reopen")
	defer teardown()

	err := w.Delete(1, "foo")
	require.NoError(t, err)

	err = w.Upsert(2, "foo", []byte("bar"))
	require.NoError(t, err)

	require.NoError(t, w.Close())

	var ids []uint64
	_, err = OpenWithPath(w.file.Name(), func(rec *Record) {
		ids = append(ids, rec.GetTxid())
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, ids)
}

func TestLoadWAL_MoreWrites(t *testing.T) {
	w, teardown := createNewWAL(t, "wal_reopen")
	defer teardown()

	err := w.Delete(1, "foo")
	require.NoError(t, err)

	require.NoError(t, w.Close())

	w, err = OpenWithPath(w.file.Name(), func(r *Record) {})
	require.NoError(t, err)

	err = w.Upsert(2, "foo", []byte("bar"))
	require.NoError(t, err)

	require.NoError(t, w.Close())

	it := createIterator(t, w.file.Name())

	rec, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), rec.GetTxid())
	require.Equal(t, "foo", rec.GetDelete().GetKey())

	rec, err = it.Next()
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, uint64(2), rec.GetTxid())
	require.Equal(t, "foo", rec.GetUpsert().GetKey())
}

func TestNewWAL_NonEmpty(t *testing.T) {
	file, err := os.CreateTemp("", "test_wal_nonempty")
	require.NoError(t, err)

	_, err = file.Write([]byte("foo"))
	require.NoError(t, err)

	_, err = New(file)
	require.ErrorIs(t, ErrWALNonEmpty, err)
}

// -------------------------------------------------------------------------------------------------

func createNewWAL(t *testing.T, pattern string) (*WAL, func()) {
	file, err := os.CreateTemp("", "test_"+pattern)
	require.NoError(t, err)

	w, err := New(file)
	require.NoError(t, err)

	return w, func() { _ = os.Remove(file.Name()) }
}
