package blt

import (
	"bytes"
	"context"
	"os"
	"sync"
	"time"

	"github.com/antgobar/kvstore/core"
	bolt "go.etcd.io/bbolt"
)

type BltStore struct {
	db            *bolt.DB
	storeName     string
	userSpaceName string
}

func (b *BltStore) Name() string {
	return b.storeName
}

func (b *BltStore) Close() error {
	return b.db.Close()
}

func (b *BltStore) TearDown() error {
	b.Close()
	err := os.Remove(b.storeName)
	if err != nil {
		return err
	}
	return nil
}

func (b *BltStore) getUserSpaceBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	bucket := tx.Bucket([]byte(b.userSpaceName))
	if bucket == nil {
		return nil, core.ErrBucketNotFound
	}
	return bucket, nil
}

func New(storeName string, userSpaceName string, timeout time.Duration) (*BltStore, error) {
	db, err := bolt.Open(storeName, 0600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(userSpaceName))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &BltStore{db: db, storeName: storeName, userSpaceName: userSpaceName}, nil
}

func (b *BltStore) Set(ctx context.Context, key string, value []byte) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := b.getUserSpaceBucket(tx)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(key), value)
	})
	return err
}

func (b *BltStore) Get(ctx context.Context, key string) ([]byte, error) {
	var val []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket, err := b.getUserSpaceBucket(tx)
		if err != nil {
			return err
		}
		v := bucket.Get([]byte(key))
		if v == nil {
			return core.ErrKeyNotFound
		}
		val = make([]byte, len(v))
		copy(val, v)
		return nil
	})
	return val, err
}

func (b *BltStore) Delete(ctx context.Context, key string) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := b.getUserSpaceBucket(tx)
		if err != nil {
			return err
		}
		return bucket.Delete([]byte(key))
	})
	return err
}

func (b *BltStore) Scan(ctx context.Context, prefix string) (<-chan []map[string][]byte, <-chan error) {
	outCh := make(chan []map[string][]byte)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		err := b.db.View(func(tx *bolt.Tx) error {
			bucket, err := b.getUserSpaceBucket(tx)
			if err != nil {
				return err
			}
			pfx := []byte(prefix)
			cursor := bucket.Cursor()
			for k, v := cursor.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); k, v = cursor.Next() {
				data := []map[string][]byte{{string(k): v}}
				select {
				case outCh <- data:
				case <-ctx.Done():
					errCh <- ctx.Err()
				}
			}
			return nil
		})
		select {
		case errCh <- err:
		case <-ctx.Done():
			errCh <- ctx.Err()
		}
	}()

	go func() {
		wg.Wait()
		close(outCh)
	}()

	return outCh, errCh
}
