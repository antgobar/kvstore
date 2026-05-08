package store

import (
	"errors"
	"sync"
)

type MemoryStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string][]byte),
	}
}

func (m *MemoryStore) Put(key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	return nil
}

func (m *MemoryStore) Get(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return v, nil
}

func (m *MemoryStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[key]; !ok {
		return errors.New("not found")
	}

	delete(m.data, key)
	return nil
}
