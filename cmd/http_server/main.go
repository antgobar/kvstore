package main

import (
	"github.com/antgobar/kvstore/internal/httpserver"
	"github.com/antgobar/kvstore/internal/store"
)

func main() {
	const addr = "localhost:8080"
	s := store.NewMemoryStore()
	httpServer := httpserver.New(addr, s)
	httpServer.Run()
}
