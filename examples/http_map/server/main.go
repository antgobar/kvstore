package main

import (
	"time"

	"github.com/antgobar/kvstore/pkg/httpserver"
	"github.com/antgobar/kvstore/pkg/stores/mapstore"
)

func main() {
	const addr = "localhost:8080"
	const requestTimeout = time.Second * 10
	s := mapstore.New()
	httpServer := httpserver.New(addr, s, requestTimeout)
	httpServer.Run()
}
