package server

import (
	"context"

	"google.golang.org/grpc"

	"github.com/antw/violin/api"
	"github.com/antw/violin/internal/storage"
)

type store interface {
	storage.ReadableStore
	storage.WritableStore
}

type server struct {
	api.UnimplementedRegisterServer
	store store
}

func New(store store, grpcOpts ...grpc.ServerOption) *grpc.Server {
	srv := &server{store: store}

	grpcSrv := grpc.NewServer(grpcOpts...)
	api.RegisterRegisterServer(grpcSrv, srv)

	return grpcSrv
}

func (s server) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	value, err := s.store.Get(req.GetKey())

	if err != nil {
		if err == storage.ErrNoSuchKey {
			err = api.ErrNoSuchKey{Key: req.Key}
		}
		return nil, err
	}

	return &api.GetResponse{
		Register: &api.KV{
			Key:   req.GetKey(),
			Value: value,
		},
	}, nil
}

func (s server) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	if err := s.store.Set(req.GetRegister().GetKey(), req.GetRegister().GetValue()); err != nil {
		return nil, err
	}

	return &api.SetResponse{}, nil
}
