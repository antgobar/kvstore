package blt

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/antgobar/kvstore/core"
)

func setupDb(t *testing.T, dbName, bucketName string) *BltStore {
	store, err := New(dbName, bucketName, time.Second*1)
	if err != nil {
		t.Fatalf("Error setting up bolt store: %v", err)
	}
	return store
}

func tearDownDb(t *testing.T, db *BltStore) {
	db.Close()
	err := os.Remove(db.storeName)
	if err != nil {
		t.Fatalf("Error tearing down bolt store: %v", err)
	}
}

func TestBltStoreGetPut(t *testing.T) {
	store := setupDb(t, "TestBlt1", "TestBlt1")
	defer tearDownDb(t, store)

	want := []byte("bar")
	err := store.Put(context.TODO(), "foo", want)
	if err != nil {
		t.Fatalf("Error storing key in bolt store: %v", err)
	}

	got, err := store.Get(context.TODO(), "foo")
	if err != nil {
		t.Fatalf("Error retrieving key: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Expected value incorrect: want %s, got %s", want, got)
	}
}

func TestBltStoreGetNonExistentReturnsErr(t *testing.T) {
	store := setupDb(t, "TestBlt2", "TestBlt2")
	defer tearDownDb(t, store)

	val, got := store.Get(context.TODO(), "foo")
	if val != nil {
		t.Fatalf("Key shouldn't exist: %s", val)
	}
	if got != core.ErrKeyNotFound {
		t.Errorf("Expected error incorrect: want %v, got %v", core.ErrKeyNotFound, got)
	}
}
