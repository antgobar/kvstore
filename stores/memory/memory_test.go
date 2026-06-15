package memory

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/antgobar/kvstore/core"
)

func TestMemoryStoreEmptyKeyReturnsErrNotFound(t *testing.T) {
	store := New()
	res, got := store.Get(context.TODO(), "foo")
	want := core.ErrKeyNotFound
	if res != nil {
		t.Errorf("Non-existent key shouldn't return data: want %q, got %q", []byte{}, res)
	}
	if got != want {
		t.Errorf("Non-existent key should return error: want %q, got %q", want, got)
	}
}

func TestMemoryStorePutValueGettable(t *testing.T) {
	store := New()
	want := []byte("bar")
	key := "foo"
	store.Set(context.TODO(), key, want)

	if got, _ := store.Get(context.TODO(), key); !bytes.Equal(got, want) {
		t.Errorf("SET value on key: foo match SET: want %q, got %q", want, got)
	}
}

func TestMemoryStoreScanKeys(t *testing.T) {
	store := New()
	store.Set(context.TODO(), "f", []byte("b"))
	store.Set(context.TODO(), "fo", []byte("ba"))
	store.Set(context.TODO(), "foo", []byte("bar"))
	store.Set(context.TODO(), "foot", []byte("bart"))

	outCh, _ := store.Scan(context.TODO(), "foo")

	want := map[string][]byte{
		"foo":  []byte("bar"),
		"foot": []byte("bart"),
	}

	got := make(map[string][]byte)

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
}

func TestMemoryStoreDeleteEmptyKeyReturnsErrNotFound(t *testing.T) {
	store := New()
	err := store.Delete(context.TODO(), "foo")
	if err != core.ErrKeyNotFound {
		t.Errorf("Expected error incorrect: want %v, got %v", core.ErrKeyNotFound, err)
	}
}

func TestMemoryStoreDeleteKeyDeletesKey(t *testing.T) {
	store := New()
	err := store.Set(context.TODO(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("data setup failed: %v", err)
	}
	err = store.Delete(context.TODO(), "foo")
	if err != nil {
		t.Errorf("Expected error incorrect: want nil, got %v", err)
	}

}
