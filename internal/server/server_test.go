package server

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/antw/violin/api"
	"github.com/antw/violin/internal/storage"
)

func setupTest(t *testing.T) (*grpc.Server, api.RegisterClient, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	store := storage.NewStore()
	server := New(&store)

	go func() {
		server.Serve(listener)
	}()

	clientConn, err := grpc.Dial(listener.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)

	return server, api.NewRegisterClient(clientConn), func() {
		server.Stop()
	}
}

func TestGetSet(t *testing.T) {
	ctx := context.Background()

	_, client, teardown := setupTest(t)
	defer teardown()

	record := &api.KV{
		Key:   "foo",
		Value: []byte("bar"),
	}

	_, err := client.Set(ctx, &api.SetRequest{Register: record})
	require.NoError(t, err)

	get, err := client.Get(ctx, &api.GetRequest{Key: "foo"})

	require.NoError(t, err)
	require.Equal(t, record.Key, get.Register.GetKey())
	require.Equal(t, record.Value, get.Register.GetValue())
}

func TestGetNotFound(t *testing.T) {
	ctx := context.Background()

	_, client, teardown := setupTest(t)
	defer teardown()

	_, err := client.Get(ctx, &api.GetRequest{Key: "key"})
	require.Errorf(t, err, "key not found: key")
}
