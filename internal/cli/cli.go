package cli

import (
	"context"
	"flag"
	"fmt"
	"log"
)

type Client interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

type Cli struct {
	Client
}

func Run(client Client) {
	args := extractInputArgs()
	fmt.Printf("Action: %s, Key: %s, Value: %s\n", args.Action, args.Key, args.Value)

	ctx := context.TODO()

	switch args.Action {
	case "put":
		err := client.Put(ctx, args.Key, []byte(args.Value))
		if err != nil {
			log.Fatalf("error putting key %s, value %s, error: %v",
				args.Key, args.Value, err)
		}
		fmt.Printf("successful put: %s - %s\n", args.Key, args.Value)

	case "get":
		value, err := client.Get(ctx, args.Key)
		if err != nil {
			log.Fatalf("error getting key %s, error: %v",
				args.Key, err)
		}
		fmt.Printf("successful get: %s - value: %s\n",
			args.Key, string(value))

	case "delete":
		err := client.Delete(ctx, args.Key)
		if err != nil {
			log.Fatalf("error deleting key %s, error: %v",
				args.Key, err)
		}
		fmt.Printf("successful delete: %s\n", args.Key)
	}
}

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
