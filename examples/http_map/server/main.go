package main

import (
	"time"

	"github.com/antgobar/kvstore/pkg/server"
	"github.com/antgobar/kvstore/pkg/store"
)

func main() {
	const addr = "localhost:8080"
	const requestTimeout = time.Second * 10
	s := store.NewMapStore()
	httpServer := server.NewHttpServer(addr, s, requestTimeout)
	httpServer.Run()
}
