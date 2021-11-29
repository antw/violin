package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/antw/violin/api"
	"github.com/antw/violin/internal/storage"
)

type server struct {
	api.UnimplementedRegisterServer
	store *storage.Store
}

func New(store *storage.Store, grpcOpts ...grpc.ServerOption) *grpc.Server {
	srv := &server{store: store}

	grpcSrv := grpc.NewServer(grpcOpts...)
	api.RegisterRegisterServer(grpcSrv, srv)

	return grpcSrv
}

func (s server) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	value, ok := s.store.Get(req.GetKey())

	if !ok {
		return nil, fmt.Errorf("key not found: %s", req.Key)
	}

	return &api.GetResponse{
		Register: &api.KV{
			Key:   req.GetKey(),
			Value: value,
		},
	}, nil
}

func (s server) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	s.store.Set(req.GetRegister().GetKey(), req.GetRegister().GetValue())
	return &api.SetResponse{}, nil
}
