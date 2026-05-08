package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

type InputArgs struct {
	Action string
	Key    string
	Value  string
}

func extractInputArgs() InputArgs {
	var input InputArgs
	flag.StringVar(&input.Action, "action", "get", "Set your key")
	flag.StringVar(&input.Key, "key", "", "Set your key")
	flag.StringVar(&input.Value, "value", "", "Set your value")
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

func main() {
	args := extractInputArgs()
	fmt.Printf("Action: %s, Key: %s, Value: %s", args.Action, args.Key, args.Value)

	resp, err := http.Get(serverAddr + "/" + args.Action)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.Body)
	defer resp.Body.Close()
}
