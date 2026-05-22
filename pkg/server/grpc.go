package server

import (
	"context"

	gserv "github.com/antgobar/kvstore/internal/genproto"
)

type GrpcServer struct {
	gserv.UnimplementedKvStoreServer
	store Storer
}

func NewGrpcServer(store Storer) *GrpcServer {
	return &GrpcServer{
		store: store,
	}
}

func (g *GrpcServer) Get(ctx context.Context, key string, value []byte) {}
