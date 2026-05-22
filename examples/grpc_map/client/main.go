package main

import (
	"time"

	"github.com/antgobar/kvstore/pkg/cli"
	"github.com/antgobar/kvstore/pkg/client"
)

const serverAddr = "localhost:50051"
const timeout = time.Second * 10

func main() {
	grpcClient := client.NewGrpcClient(serverAddr)
	cli.Run(grpcClient)
}
