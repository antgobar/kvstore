package memory

import (
	"context"
	"maps"
	"strings"
	"sync"

	"github.com/antgobar/kvstore/core"
)

type MemoryStore struct {
	data          map[string][]byte
	mu            sync.RWMutex
	scanBatchSize int
}

func New() *MemoryStore {
	return &MemoryStore{
		data:          make(map[string][]byte),
		scanBatchSize: 1,
	}
}

func (m *MemoryStore) Set(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	return nil
}

func (m *MemoryStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.data[key]
	if !ok {
		return nil, core.ErrKeyNotFound
	}
	return v, nil
}

func (m *MemoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *MemoryStore) Scan(ctx context.Context, prefix string) (<-chan []map[string][]byte, <-chan error) {
	outCh := make(chan []map[string][]byte)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(errCh)
		m.mu.RLock()
		snapshot := maps.Clone(m.data)
		m.mu.RUnlock()

		buff := make([]map[string][]byte, 0)

		for key, value := range snapshot {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			buff = append(buff, map[string][]byte{key: value})

			if len(buff) < m.scanBatchSize {
				continue
			}

			if len(buff) >= m.scanBatchSize {
				select {
				case outCh <- buff:
					buff = make([]map[string][]byte, 0)
				case <-ctx.Done():
					errCh <- ctx.Err()
				}
			}

		}
		if len(buff) > 0 {
			select {
			case outCh <- buff:
			case <-ctx.Done():
				errCh <- ctx.Err()
			}
		}
	}()

	go func() {
		wg.Wait()
		close(outCh)
	}()

	return outCh, errCh
}
