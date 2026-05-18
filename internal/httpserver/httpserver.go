package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	custom_errors "github.com/antgobar/kvstore/internal/errors"
	"github.com/antgobar/kvstore/internal/transport"
)

type Store interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

type Server struct {
	Addr           string
	Store          Store
	RequestTimeout time.Duration
}

func New(addr string, store Store, requestTimeout time.Duration) *Server {
	return &Server{
		Addr:           addr,
		Store:          store,
		RequestTimeout: requestTimeout,
	}
}

func (s *Server) Run() {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}

	mux.HandleFunc("POST /put", s.handlePut)
	mux.HandleFunc("POST /get", s.handleGet)
	mux.HandleFunc("POST /delete", s.handleDelete)

	fmt.Println("Running kvstore http server on", s.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s", err)
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
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
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
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
