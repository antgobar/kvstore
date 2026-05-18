package main

import (
	"time"

	"github.com/antgobar/kvstore/internal/httpserver"
	"github.com/antgobar/kvstore/internal/store"
)

func main() {
	const addr = "localhost:8080"
	const requestTimeout = time.Second * 10
	s := store.NewMemoryStore()
	httpServer := httpserver.New(addr, s, requestTimeout)
	httpServer.Run()
}
