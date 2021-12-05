package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	store := NewStore()

	err := store.Set("foo", []byte("bar"))
	require.NoError(t, err)

	value, ok := store.Get("foo")
	require.True(t, ok)
	require.Equal(t, []byte("bar"), value)
}

func TestEmpty(t *testing.T) {
	store := NewStore()

	_, ok := store.Get("foo")
	require.False(t, ok)
}
