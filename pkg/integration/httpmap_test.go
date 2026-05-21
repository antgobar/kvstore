package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/antgobar/kvstore/pkg/client"
	"github.com/antgobar/kvstore/pkg/server"
	"github.com/antgobar/kvstore/pkg/store"
)

func TestEndToEndPutKeyGettable(t *testing.T) {
	httpClient := client.NewHttpClient("http://localhost:8080", time.Second*5)
	mapStore := store.NewMapStore()
	httpServer := server.NewHttpServer("localhost:8080", mapStore, time.Second*5)

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

func TestEndToEndPutKeyUpdatedRetrievable(t *testing.T) {
	httpClient := client.NewHttpClient("http://localhost:8080", time.Second*5)
	mapStore := store.NewMapStore()
	httpServer := server.NewHttpServer("localhost:8080", mapStore, time.Second*5)

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

func TestEndToEndGetNonExistentKeyErrorsNotFound(t *testing.T) {
	httpClient := client.NewHttpClient("http://localhost:8080", time.Second*5)
	mapStore := store.NewMapStore()
	httpServer := server.NewHttpServer("localhost:8080", mapStore, time.Second*5)

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

func containsNotFound(msg string) bool {
	return bytes.Contains(bytes.ToLower([]byte(msg)), []byte("not found"))
}
