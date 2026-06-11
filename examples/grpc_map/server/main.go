package main

import (
	"time"

	"github.com/antgobar/kvstore/stores/mapstore"
	"github.com/antgobar/kvstore/transport/grpc/server"
)

func main() {
	const addr = "localhost:50051"
	const requestTimeout = time.Second * 10
	s := mapstore.New()
	grpcServer := server.NewGrpcServer(addr, s, requestTimeout)
	grpcServer.Run()
}
