package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	store "github.com/antgobar/kvstore/stores/mapstore"
	client "github.com/antgobar/kvstore/transport/http/client"
	server "github.com/antgobar/kvstore/transport/http/server"
)

const httpTestServerAddr = "http://localhost:8090"
const httpClientRequestAddr = "localhost:8090"

func TestHttpMapEndToEndPutKeyGettable(t *testing.T) {
	httpClient := client.New(httpTestServerAddr, time.Second*5)
	mapStore := store.New()
	httpServer := server.New(httpClientRequestAddr, mapStore, time.Second*5)

	go httpServer.Run()
	defer httpServer.Stop()

	time.Sleep(100 * time.Millisecond)

	ctx := context.TODO()
	if err := httpClient.Put(ctx, "foo", []byte("bar")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	want := []byte("bar")
	got, err := httpClient.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestHttpMapEndToEndPutKeyUpdatedRetrievable(t *testing.T) {
	httpClient := client.New(httpTestServerAddr, time.Second*5)
	mapStore := store.New()
	httpServer := server.New(httpClientRequestAddr, mapStore, time.Second*5)

	go httpServer.Run()
	defer httpServer.Stop()

	time.Sleep(100 * time.Millisecond)

	ctx := context.TODO()
	if err := httpClient.Put(ctx, "foo", []byte("bar")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := httpClient.Put(ctx, "foo", []byte("baz")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	want := []byte("baz")
	got, err := httpClient.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestHttpMapEndToEndGetNonExistentKeyErrorsNotFound(t *testing.T) {
	httpClient := client.New(httpTestServerAddr, time.Second*5)
	mapStore := store.New()
	httpServer := server.New(httpClientRequestAddr, mapStore, time.Second*5)

	go httpServer.Run()
	defer httpServer.Stop()

	time.Sleep(100 * time.Millisecond)

	ctx := context.TODO()
	_, err := httpClient.Get(ctx, "foo")
	if err == nil {
		t.Fatalf("expected error for non-existent key, got nil")
	}
	if !containsNotFound(err.Error()) {
		t.Errorf("expected error to contain 'not found', got: %v", err)
	}
}
