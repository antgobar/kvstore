package main

import (
	"time"

	"github.com/antgobar/kvstore/pkg/server"
	"github.com/antgobar/kvstore/pkg/store"
)

func main() {
	const addr = ":50051"
	const requestTimeout = time.Second * 10
	s := store.NewMapStore()
	grpcServer := server.NewGrpcServer(addr, s, requestTimeout)
	grpcServer.Run()
}
