package main

import (
	"time"

	store "github.com/antgobar/kvstore/stores/memory"
	"github.com/antgobar/kvstore/transport/grpc/server"
)

func main() {
	const addr = "localhost:50051"
	const requestTimeout = time.Second * 10
	s := store.New()
	grpcServer := server.NewGrpcServer(addr, s, requestTimeout)
	grpcServer.Run()
}
