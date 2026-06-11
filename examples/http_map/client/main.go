package main

import (
	"time"

	"github.com/antgobar/kvstore/cli"
	"github.com/antgobar/kvstore/transport/http/client"
)

const serverAddr = "http://localhost:8080"
const timeout = time.Second * 10

func main() {
	httpClient := client.New(serverAddr, timeout)
	cli.Run(httpClient)
}
