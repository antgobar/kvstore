package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/antgobar/kvstore/internal/store"
)

func main() {
	const addr = "localhost:8080"
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	s := store.NewMemoryStore()

	mux.HandleFunc("GET /info", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello from Go HTTP server\n")
		fmt.Fprintf(w, "%s %s %s", r.RemoteAddr, r.Method, r.URL)
	})

	mux.HandleFunc("GET /put", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		key := q.Get("key")
		value := q.Get("value")
		s.Put(key, []byte(value))
		fmt.Fprintf(w, "PUT - key:%s value:%s", key, value)
	})

	mux.HandleFunc("GET /get", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		key := q.Get("key")
		value, err := s.Get(key)
		if err != nil {
			http.Error(w, "Error retrieving key: "+key+" "+err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "GET - key:%s value:%s", key, value)
	})

	fmt.Println("Running kvstore server on", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s", err)
	}
}
