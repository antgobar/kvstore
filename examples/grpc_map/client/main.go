package main

import (
	"time"

	"github.com/antgobar/kvstore/pkg/cli"
	"github.com/antgobar/kvstore/pkg/grpcclient"
)

const serverAddr = "localhost:50051"
const timeout = time.Second * 10

func main() {
	grpcClient := grpcclient.New(serverAddr, timeout)
	cli.Run(grpcClient)
}
