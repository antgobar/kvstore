package main

import (
	"time"

	"github.com/antgobar/kvstore/pkg/stores/mapstore"
	"github.com/antgobar/kvstore/pkg/transport/grpc/server"
)

func main() {
	const addr = "localhost:50051"
	const requestTimeout = time.Second * 10
	s := mapstore.New()
	grpcServer := server.NewGrpcServer(addr, s, requestTimeout)
	grpcServer.Run()
}
