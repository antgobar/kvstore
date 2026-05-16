package httpserver

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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

	mux.HandleFunc("GET /put", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		key := q.Get("key")
		value := q.Get("value")
		s.Store.Put(key, []byte(value))
		fmt.Fprintf(w, "PUT - key:%s value:%s", key, value)
	})

	mux.HandleFunc("GET /get", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		key := q.Get("key")
		value, err := s.Store.Get(key)
		if err != nil {
			http.Error(w, "Error retrieving key: "+key+" "+err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "GET - key:%s value:%s", key, value)
	})

	fmt.Println("Running kvstore http server on", s.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s", err)
	}
}

type keyValue struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func (h *handler) handlePut(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var keyVal keyValue

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

type key struct {
	Key string `json:"key"`
}

func (h *handler) handleGet(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var k key

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

	resp := keyValue{Key: k.Key, Value: value}
	json.NewEncoder(w).Encode(resp)
}

func (h *handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var k key

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
