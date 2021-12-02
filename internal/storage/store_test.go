package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	store := NewStore()

	store.Set("foo", []byte("bar"))

	value, ok := store.Get("foo")
	require.True(t, ok)
	require.Equal(t, []byte("bar"), value)
}

func TestEmpty(t *testing.T) {
	store := NewStore()

	_, ok := store.Get("foo")
	require.False(t, ok)
}
