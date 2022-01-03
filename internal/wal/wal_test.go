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

// -------------------------------------------------------------------------------------------------

func createNewWAL(t *testing.T, pattern string) (*WAL, func()) {
	file, err := os.CreateTemp("", "test_"+pattern)
	require.NoError(t, err)

	w, err := NewWAL(file)
	require.NoError(t, err)

	return w, func() { _ = os.Remove(file.Name()) }
}
