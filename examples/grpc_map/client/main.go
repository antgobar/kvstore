package main

import (
	"time"

	"github.com/antgobar/kvstore/cli"
	"github.com/antgobar/kvstore/transport/grpc/client"
)

const serverAddr = "localhost:50051"
const timeout = time.Second * 10

func main() {
	grpcClient := client.New(serverAddr, timeout)
	cli.Run(grpcClient)
}
