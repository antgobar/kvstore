package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/antgobar/kvstore/internal/httpclient"
)

type InputArgs struct {
	Action string
	Key    string
	Value  string
}

func extractInputArgs() InputArgs {
	var input InputArgs
	flag.StringVar(&input.Action, "a", "get", "Set your key")
	flag.StringVar(&input.Key, "k", "", "Set your key")
	flag.StringVar(&input.Value, "v", "", "Set your value")
	flag.Parse()

	switch input.Action {
	case "get":
		if input.Key == "" || input.Value != "" {
			log.Fatal("Get action requires a key and no value!")
		}
	case "put":
		if input.Key == "" || input.Value == "" {
			log.Fatal("Put action requires both key and value!")
		}
	case "delete":
		if input.Key == "" || input.Value != "" {
			log.Fatal("Delete actions requires a key and no value!")
		}
	default:
		log.Fatal("Invalid action!")
	}

	return input
}

const serverAddr = "http://localhost:8080"
const timeout = time.Second * 10

func main() {
	args := extractInputArgs()
	fmt.Printf("Action: %s, Key: %s, Value: %s\n", args.Action, args.Key, args.Value)

	httpClient := httpclient.New(serverAddr, timeout)
	ctx := context.TODO()

	switch args.Action {
	case "put":
		err := httpClient.Put(ctx, args.Key, []byte(args.Value))
		if err != nil {
			log.Fatalf("error putting key %s, value %s, error: %v",
				args.Key, args.Value, err)
		}
		fmt.Printf("successful put: %s - %s\n", args.Key, args.Value)

	case "get":
		value, err := httpClient.Get(ctx, args.Key)
		if err != nil {
			log.Fatalf("error getting key %s, error: %v",
				args.Key, err)
		}
		fmt.Printf("successful get: %s - value: %s\n",
			args.Key, string(value))

	case "delete":
		err := httpClient.Delete(ctx, args.Key)
		if err != nil {
			log.Fatalf("error deleting key %s, error: %v",
				args.Key, err)
		}
		fmt.Printf("successful delete: %s\n", args.Key)
	}
}
