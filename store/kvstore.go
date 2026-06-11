package sss

// import (
// 	"context"
// 	"fmt"
// 	"strings"
// 	"sync"
// 	"time"

// 	kvstore "github.com/antgobar/kvstore/errors"
// )

// type Entry struct {
// 	Key     string
// 	Value   []byte
// 	TTL     time.Duration
// 	Version time.Time
// }

// func (e Entry) String() string {
// 	return fmt.Sprintf("Key: %s - Value: %s", e.Key, e.Value)
// }

// type Store interface {
// 	Get(ctx context.Context, key string) (Entry, error)
// 	Set(ctx context.Context, key string, value Entry) error
// 	Delete(ctx context.Context, key string) error
// 	Scan(ctx context.Context, prefix string) (<-chan []Entry, <-chan error)
// }

// type UserDataStore struct {
// 	data      map[string]Entry
// 	mu        sync.RWMutex
// 	ScanBatch int
// }

// func (u *UserDataStore) SetEntries(entries ...Entry) {
// 	for _, entry := range entries {
// 		u.Set(context.TODO(), entry.Key, entry)
// 	}
// }

// func New() *UserDataStore {
// 	return &UserDataStore{
// 		data: make(map[string]Entry),
// 	}
// }

// func (m *UserDataStore) Set(_ context.Context, key string, entry Entry) error {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
// 	m.data[key] = entry
// 	return nil
// }

// func (m *UserDataStore) Get(_ context.Context, key string) (Entry, error) {
// 	m.mu.RLock()
// 	defer m.mu.RUnlock()

// 	v, ok := m.data[key]
// 	if !ok {
// 		return Entry{}, kvstore.ErrKeyNotFound
// 	}
// 	return v, nil
// }

// func (m *UserDataStore) Delete(_ context.Context, key string) error {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()

// 	if _, ok := m.data[key]; !ok {
// 		return kvstore.ErrKeyNotFound
// 	}

// 	delete(m.data, key)
// 	return nil
// }

// func (u *UserDataStore) Scan(ctx context.Context, prefix string) (<-chan []Entry, <-chan error) {
// 	outCh := make(chan []Entry)
// 	errCh := make(chan error, 1)

// 	var wg sync.WaitGroup

// 	wg.Add(1)

// 	go func() {
// 		defer wg.Done()
// 		defer close(errCh)

// 		buff := []Entry{}

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

// func Scan(store Store, prefix string) {
// 	fmt.Println("SCANCH FUNC")
// 	ctx := context.TODO()
// 	i := 0
// 	outCh, _ := store.Scan(ctx, prefix)
// 	for vals := range outCh {
// 		for idx, val := range vals {
// 			fmt.Println(i, idx, val.Key)
// 		}
// 		i += 1
// 	}
// }

// func main() {
// 	u := New()
// 	u.SetEntries(
// 		Entry{Key: "f", Value: []byte("shoo")},
// 		Entry{Key: "fo", Value: []byte("shoo")},
// 		Entry{Key: "foo", Value: []byte("shoo")},
// 		Entry{Key: "fap", Value: []byte("shoo")},
// 	)
// 	u.ScanBatch = 2

// 	Scan(u, "f")

// 	val, err := u.Get(context.TODO(), "f")
// 	fmt.Printf("RESULT: %s - ERR: %s", val, err)

// }
