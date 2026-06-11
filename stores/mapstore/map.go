package mapstore

import (
	"context"
	"sync"

	"github.com/antgobar/kvstore/core"
)

type MapStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func New() *MapStore {
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
		return nil, core.ErrKeyNotFound
	}
	return v, nil
}

func (m *MapStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[key]; !ok {
		return core.ErrKeyNotFound
	}

	delete(m.data, key)
	return nil
}
