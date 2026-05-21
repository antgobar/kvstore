package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	custom_errors "github.com/antgobar/kvstore/pkg/errors"
	"github.com/antgobar/kvstore/pkg/transport"
)

type Storer interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

type HttpServer struct {
	Addr           string
	Store          Storer
	RequestTimeout time.Duration
	server         *http.Server
}

func NewHttpServer(addr string, store Storer, requestTimeout time.Duration) *HttpServer {
	return &HttpServer{
		Addr:           addr,
		Store:          store,
		RequestTimeout: requestTimeout,
	}
}

func (s *HttpServer) Run() {
	mux := http.NewServeMux()
	s.server = &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}

	mux.HandleFunc("POST /put", s.handlePut)
	mux.HandleFunc("POST /get", s.handleGet)
	mux.HandleFunc("POST /delete", s.handleDelete)

	fmt.Println("Running kvstore http server on", s.Addr)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s", err)
	}
}

func (s *HttpServer) Stop() {
	if s.server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		log.Printf("HTTP server Shutdown: %v", err)
	}
}

func (s *HttpServer) handlePut(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var keyVal transport.KeyValuePayload

	if err := decoder.Decode(&keyVal); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.RequestTimeout)
	defer cancel()
	if err := s.Store.Put(ctx, keyVal.Key, keyVal.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
}

func (s *HttpServer) handleGet(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var k transport.KeyPayload

	if err := decoder.Decode(&k); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.RequestTimeout)
	defer cancel()

	value, err := s.Store.Get(ctx, k.Key)
	if err != nil {
		if err == custom_errors.ErrKeyNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := transport.ValuePayload{Value: value}
	if err = json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

}

func (s *HttpServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var k transport.KeyPayload

	if err := decoder.Decode(&k); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.RequestTimeout)
	defer cancel()
	if err := s.Store.Delete(ctx, k.Key); err != nil {
		if err == custom_errors.ErrKeyNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
}
