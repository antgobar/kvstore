package main

import (
	"time"

	store "github.com/antgobar/kvstore/stores/memory"
	"github.com/antgobar/kvstore/transport/http/server"
)

func main() {
	const addr = "localhost:8080"
	const requestTimeout = time.Second * 10
	s := store.New()
	httpServer := server.New(addr, s, requestTimeout)
	httpServer.Run()
}
