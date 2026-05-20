package store

import (
	"context"
	"sync"

	custom_errors "github.com/antgobar/kvstore/pkg/errors"
)

type MapStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewMapStore() *MapStore {
	return &MapStore{
		data: make(map[string][]byte),
	}
}

func (m *MapStore) Put(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	return nil
}

func (m *MapStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.data[key]
	if !ok {
		return nil, custom_errors.ErrKeyNotFound
	}
	return v, nil
}

func (m *MapStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[key]; !ok {
		return custom_errors.ErrKeyNotFound
	}

	delete(m.data, key)
	return nil
}
