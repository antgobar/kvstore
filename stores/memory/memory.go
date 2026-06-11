package memory

import (
	"context"
	"sync"

	"github.com/antgobar/kvstore/core"
)

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

	if _, ok := m.data[key]; !ok {
		return core.ErrKeyNotFound
	}

	delete(m.data, key)
	return nil
}

// func (u *MemoryStore) Scan(ctx context.Context, prefix string) (<-chan []byte, <-chan error) {
// 	outCh := make(chan []byte)
// 	errCh := make(chan error, 1)

// 	var wg sync.WaitGroup

// 	wg.Add(1)

// 	go func() {
// 		defer wg.Done()
// 		defer close(errCh)

// 		buff := [][]byte{}

// 		for _, entry := range u.data {
// 			if !strings.HasPrefix(entry.Key, prefix) {
// 				continue
// 			}
// 			buff = append(buff, entry)

// 			if len(buff) < u.ScanBatch {
// 				continue
// 			}

// 			if len(buff) > 0 {
// 				select {
// 				case outCh <- buff:
// 				case <-ctx.Done():
// 					errCh <- ctx.Err()
// 				}
// 			}

// 		}
// 		if len(buff) > 0 {
// 			select {
// 			case outCh <- buff:
// 			case <-ctx.Done():
// 				errCh <- ctx.Err()
// 			}
// 		}
// 	}()

// 	go func() {
// 		wg.Wait()
// 		close(outCh)
// 	}()

// 	return outCh, errCh
// }
