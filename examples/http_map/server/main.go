package main

import (
	"time"

	"github.com/antgobar/kvstore/stores/mapstore"
	"github.com/antgobar/kvstore/transport/http/server"
)

func main() {
	const addr = "localhost:8080"
	const requestTimeout = time.Second * 10
	s := mapstore.New()
	httpServer := server.New(addr, s, requestTimeout)
	httpServer.Run()
}
