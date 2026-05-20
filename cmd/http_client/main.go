package main

import (
	"time"

	"github.com/antgobar/kvstore/pkg/cli"
	"github.com/antgobar/kvstore/pkg/client"
)

const serverAddr = "http://localhost:8080"
const timeout = time.Second * 10

func main() {
	httpClient := client.NewHttpClient(serverAddr, timeout)
	cli.Run(httpClient)
}
