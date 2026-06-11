package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	store "github.com/antgobar/kvstore/stores/memory"
	client "github.com/antgobar/kvstore/transport/grpc/client"
	server "github.com/antgobar/kvstore/transport/grpc/server"
)

const grpcTestServerAddr = "localhost:50051"
const grpcClientRequestAddr = "localhost:50051"

func TestGrpcMapEndToEndSetKeyGettable(t *testing.T) {
	grpcClient := client.New(grpcTestServerAddr, time.Second*5)
	mapStore := store.New()
	grpcServer := server.NewGrpcServer(grpcClientRequestAddr, mapStore, time.Second*5)

	go grpcServer.Run()
	defer grpcServer.Stop()

	time.Sleep(200 * time.Millisecond)

	ctx := context.TODO()
	if err := grpcClient.Set(ctx, "foo", []byte("bar")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	want := []byte("bar")
	got, err := grpcClient.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestGrpcMapEndToEndSetKeyUpdatedRetrievable(t *testing.T) {
	grpcClient := client.New(grpcTestServerAddr, time.Second*5)
	mapStore := store.New()
	grpcServer := server.NewGrpcServer(grpcClientRequestAddr, mapStore, time.Second*5)

	go grpcServer.Run()
	defer grpcServer.Stop()

	time.Sleep(200 * time.Millisecond)

	ctx := context.TODO()
	if err := grpcClient.Set(ctx, "foo", []byte("bar")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := grpcClient.Set(ctx, "foo", []byte("baz")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	want := []byte("baz")
	got, err := grpcClient.Get(ctx, "foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestGrpcMapEndToEndGetNonExistentKeyErrorsNotFound(t *testing.T) {
	httpClient := client.New(grpcTestServerAddr, time.Second*5)
	mapStore := store.New()
	httpServer := server.NewGrpcServer(grpcClientRequestAddr, mapStore, time.Second*5)

	go httpServer.Run()
	defer httpServer.Stop()

	time.Sleep(200 * time.Millisecond)

	ctx := context.TODO()
	_, err := httpClient.Get(ctx, "foo")
	if err == nil {
		t.Fatalf("expected error for non-existent key, got nil")
	}
	if !containsNotFound(err.Error()) {
		t.Errorf("expected error to contain 'not found', got: %v", err)
	}
}
