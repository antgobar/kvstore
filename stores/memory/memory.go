package memory

import (
	"context"
	"maps"
	"strings"
	"sync"

	"github.com/antgobar/kvstore/core"
)

const maxPageSize = 50

type MemoryStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func New() *MemoryStore {
	return &MemoryStore{
		data: make(map[string][]byte),
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

func freshPage() []map[string][]byte {
	return make([]map[string][]byte, 0)
}

func (m *MemoryStore) Scan(ctx context.Context, prefix string) (<-chan []map[string][]byte, <-chan error) {
	outCh := make(chan []map[string][]byte)
	errCh := make(chan error, 1)

	m.mu.RLock()
	snapshot := maps.Clone(m.data)
	m.mu.RUnlock()

	var wg sync.WaitGroup

	wg.Go(func() {
		defer close(errCh)
		page := freshPage()
		for key, value := range snapshot {
			if !strings.HasPrefix(key, prefix) {
				continue
			}

			if len(page) >= maxPageSize {
				select {
				case outCh <- page:
					page = freshPage()
				case <-ctx.Done():
					errCh <- ctx.Err()
				}
			}

			page = append(page, map[string][]byte{key: value})

			if len(page) < maxPageSize {
				continue
			}

			if len(page) >= maxPageSize {
				select {
				case outCh <- page:
					page = make([]map[string][]byte, 0)
				case <-ctx.Done():
					errCh <- ctx.Err()
				}
			}

		}
		if len(page) > 0 {
			select {
			case outCh <- page:
			case <-ctx.Done():
				errCh <- ctx.Err()
			}
		}
	})

	go func() {
		wg.Wait()
		close(outCh)
	}()

	return outCh, errCh
}
