package main

import (
	"time"

	"github.com/antgobar/kvstore/pkg/grpcserver"
	"github.com/antgobar/kvstore/pkg/stores/mapstore"
)

func main() {
	const addr = "localhost:50051"
	const requestTimeout = time.Second * 10
	s := mapstore.New()
	grpcServer := grpcserver.NewGrpcServer(addr, s, requestTimeout)
	grpcServer.Run()
}
