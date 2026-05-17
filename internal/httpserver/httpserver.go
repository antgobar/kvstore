package httpserver

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/antgobar/kvstore/internal/transport"
)

type Store interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

type Server struct {
	Addr  string
	Store Store
}

func New(addr string, store Store) *Server {
	return &Server{
		Addr:  addr,
		Store: store,
	}
}

type handler struct {
	store Store
}

func (s *Server) Run() {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}

	h := handler{store: s.Store}

	mux.HandleFunc("POST /put", h.handlePut)
	mux.HandleFunc("POST /get", h.handleGet)
	mux.HandleFunc("POST /delete", h.handleDelete)

	fmt.Println("Running kvstore http server on", s.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s", err)
	}
}

func (h *handler) handlePut(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var keyVal transport.KeyValuePayload

	if err := decoder.Decode(&keyVal); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.store.Put(keyVal.Key, keyVal.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
}

func (h *handler) handleGet(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var k transport.KeyPayload

	if err := decoder.Decode(&k); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	value, err := h.store.Get(k.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := transport.ValuePayload{Value: value}
	json.NewEncoder(w).Encode(resp)
}

func (h *handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var k transport.KeyPayload

	if err := decoder.Decode(&k); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := h.store.Delete(k.Key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
}
