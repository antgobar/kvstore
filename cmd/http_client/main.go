package main

import (
	"time"

	"github.com/antgobar/kvstore/internal/cli"
	"github.com/antgobar/kvstore/internal/httpclient"
)

const serverAddr = "http://localhost:8080"
const timeout = time.Second * 10

func main() {
	httpClient := httpclient.New(serverAddr, timeout)
	cli.Run(httpClient)
}
