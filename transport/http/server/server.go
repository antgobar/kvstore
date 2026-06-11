package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/antgobar/kvstore/core"
	"github.com/antgobar/kvstore/transport/http/payload"
)

type HttpServer struct {
	Addr           string
	Store          core.Store
	RequestTimeout time.Duration
	server         *http.Server
}

func New(addr string, store core.Store, requestTimeout time.Duration) *HttpServer {
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

	mux.HandleFunc("POST /set", s.handleSet)
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

func (s *HttpServer) handleSet(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var keyVal payload.KeyValuePayload

	if err := decoder.Decode(&keyVal); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.RequestTimeout)
	defer cancel()
	if err := s.Store.Set(ctx, keyVal.Key, keyVal.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
}

func (s *HttpServer) handleGet(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var k payload.KeyPayload

	if err := decoder.Decode(&k); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.RequestTimeout)
	defer cancel()

	value, err := s.Store.Get(ctx, k.Key)
	if err != nil {
		if err == core.ErrKeyNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := payload.ValuePayload{Value: value}
	if err = json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

}

func (s *HttpServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var k payload.KeyPayload

	if err := decoder.Decode(&k); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.RequestTimeout)
	defer cancel()
	if err := s.Store.Delete(ctx, k.Key); err != nil {
		if err == core.ErrKeyNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
}
