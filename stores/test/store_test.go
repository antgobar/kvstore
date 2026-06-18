package test

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/antgobar/kvstore/core"
	"github.com/antgobar/kvstore/stores/memory"
)

func TestStoreImplementationsGetPut(t *testing.T) {
	memoryStore := memory.New()
	boltStore := setupBoltDb(t)
	defer tearDownBoltDb(t, boltStore)

	key := "foo"
	want := []byte("bar")

	tests := []struct {
		name  string
		store core.Store
	}{
		{
			name:  "memory store",
			store: memoryStore,
		},
		{
			name:  "bolt store",
			store: boltStore,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.store.Set(context.TODO(), key, want)
			if err != nil {
				t.Fatalf("error setting value: %v", err)
			}
			got, err := tt.store.Get(context.TODO(), key)
			if err != nil {
				t.Fatalf("error retrieving value: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("Expected value incorrect: want %s, got %s", want, got)
			}
		})
	}
}

func TestStoreDeleteKeyDeletes(t *testing.T) {
	memoryStore := memory.New()
	boltStore := setupBoltDb(t)
	defer tearDownBoltDb(t, boltStore)

	key := "foo"
	want := []byte("bar")

	tests := []struct {
		name  string
		store core.Store
	}{
		{
			name:  "memory store",
			store: memoryStore,
		},
		{
			name:  "bolt store",
			store: boltStore,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.store.Set(context.TODO(), key, want)
			if err != nil {
				t.Fatalf("error setting value: %v", err)
			}
			err = tt.store.Delete(context.TODO(), key)
			if err != nil {
				t.Fatalf("error deleting key %s: %v", key, err)
			}
		})
	}
}

func TestStoreImplementationsGetNonExistentKeyErrors(t *testing.T) {
	memoryStore := memory.New()
	boltStore := setupBoltDb(t)
	defer tearDownBoltDb(t, boltStore)

	key := "foo"
	want := core.ErrKeyNotFound

	tests := []struct {
		name  string
		store core.Store
	}{
		{
			name:  "memory store",
			store: memoryStore,
		},
		{
			name:  "bolt store",
			store: boltStore,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := tt.store.Get(context.TODO(), key)
			if got != want {
				t.Errorf("Expected error incorrect: want %v, got %v", want, got)
			}
		})
	}
}

func TestStoreImplementationsScanReturnsPrefixValues(t *testing.T) {
	memoryStore := memory.New()
	boltStore := setupBoltDb(t)
	defer tearDownBoltDb(t, boltStore)

	storeData := map[string][]byte{
		"f":    []byte("b"),
		"fo":   []byte("ba"),
		"foo":  []byte("bar"),
		"foot": []byte("bart"),
	}
	want := map[string][]byte{
		"foo":  []byte("bar"),
		"foot": []byte("bart"),
	}

	tests := []struct {
		name  string
		store core.ScanStore
	}{
		{
			name:  "memory store",
			store: memoryStore,
		},
		{
			name:  "bolt store",
			store: boltStore,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range storeData {
				err := tt.store.Set(context.TODO(), k, v)
				if err != nil {
					t.Fatalf("error setting key: %s: err %v", k, err)
				}
			}
			got := make(map[string][]byte)
			outCh, _ := tt.store.Scan(context.TODO(), "foo")
			gotResultSize := 0
			for vals := range outCh {
				for _, data := range vals {
					for k, v := range data {
						gotResultSize += 1
						got[k] = v
					}
				}
			}
			if !reflect.DeepEqual(want, got) {
				t.Errorf("Incorrect results: want %s, got %s", want, got)
			}
			wantResultSize := 2
			if gotResultSize != wantResultSize {
				t.Errorf("Expected keys incorrect: want %d, got %d", wantResultSize, gotResultSize)
			}
		})
	}

}
