package server

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/antw/violin/api"
	"github.com/antw/violin/internal/storage"
)

func setupTest(t *testing.T) (*grpc.Server, api.RegisterClient, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	store := storage.NewStore()
	server := New(store)

	go func() {
		_ = server.Serve(listener)
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

	value, err := client.Get(ctx, &api.GetRequest{Key: "key"})
	require.Nil(t, value)

	got := status.Code(err)
	want := status.Code(api.ErrNoSuchKey{}.GRPCStatus().Err())

	require.Equal(t, want, got)
}

func TestServer_Delete(t *testing.T) {
	ctx := context.Background()

	_, client, teardown := setupTest(t)
	defer teardown()

	record := &api.KV{
		Key:   "foo",
		Value: []byte("bar"),
	}

	_, err := client.Set(ctx, &api.SetRequest{Register: record})
	require.NoError(t, err)

	resp, err := client.Delete(ctx, &api.DeleteRequest{Key: "foo"})
	require.NoError(t, err)

	require.Equal(t, uint64(1), resp.GetDeleted())
	require.NotNil(t, resp.GetRegister())
	require.Equal(t, "foo", resp.GetRegister().GetKey())
	require.Equal(t, []byte("bar"), resp.GetRegister().GetValue())
}

func TestServer_Delete_NotExist(t *testing.T) {
	ctx := context.Background()

	_, client, teardown := setupTest(t)
	defer teardown()

	resp, err := client.Delete(ctx, &api.DeleteRequest{Key: "foo"})
	require.NoError(t, err)

	require.Equal(t, uint64(0), resp.GetDeleted())
	require.Nil(t, resp.GetRegister())
}

func TestServer_Get_DeletedKey(t *testing.T) {
	ctx := context.Background()

	_, client, teardown := setupTest(t)
	defer teardown()

	record := &api.KV{
		Key:   "foo",
		Value: []byte("bar"),
	}

	_, err := client.Set(ctx, &api.SetRequest{Register: record})
	require.NoError(t, err)

	_, err = client.Delete(ctx, &api.DeleteRequest{Key: "foo"})
	require.NoError(t, err)

	get, err := client.Get(ctx, &api.GetRequest{Key: "foo"})
	require.Nil(t, get)

	got := status.Code(err)
	want := status.Code(api.ErrNoSuchKey{}.GRPCStatus().Err())

	require.Equal(t, want, got)
}
