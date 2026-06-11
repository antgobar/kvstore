package main

import (
	"time"

	"github.com/antgobar/kvstore/pkg/stores/mapstore"
	"github.com/antgobar/kvstore/pkg/transport/http/server"
)

func main() {
	const addr = "localhost:8080"
	const requestTimeout = time.Second * 10
	s := mapstore.New()
	httpServer := server.New(addr, s, requestTimeout)
	httpServer.Run()
}
