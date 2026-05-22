package server

import (
	"context"
	"fmt"
	"time"

	gserv "github.com/antgobar/kvstore/internal/genproto"
)

type GrpcServer struct {
	gserv.UnimplementedKvStoreServer
	store   Storer
	addr    string
	timeout time.Duration
}

func NewGrpcServer(addr string, store Storer, timeout time.Duration) *GrpcServer {
	return &GrpcServer{
		store:   store,
		addr:    addr,
		timeout: timeout,
	}
}

func (g *GrpcServer) Get(ctx context.Context, key string, value []byte) {}

func (g *GrpcServer) Run() {
	fmt.Println("GRPC server running...", g.addr)
}
