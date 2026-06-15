package blt

import (
	"log"
	"time"

	bolt "go.etcd.io/bbolt"
)

type BltStore struct {
}

func New(timeout time.Duration) *BltStore {
	db, err := bolt.Open("boltKvStore.db", 0600, &bolt.Options{Timeout: timeout})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	return &BltStore{}
}
