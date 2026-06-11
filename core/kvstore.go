package core

import (
	"context"
	"fmt"
	"time"
)

type Entry struct {
	Key     string
	Value   []byte
	TTL     time.Duration
	Version time.Time
}

func (e Entry) String() string {
	return fmt.Sprintf("Key: %s - Value: %s", e.Key, e.Value)
}

type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

type Scanner interface {
	Scan(ctx context.Context, prefix string) (<-chan []Entry, <-chan error)
}

type ScanStore interface {
	Store
	Scanner
}
