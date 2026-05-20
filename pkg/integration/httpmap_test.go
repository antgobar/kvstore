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

func TestEndToEndPutKey(t *testing.T) {
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
