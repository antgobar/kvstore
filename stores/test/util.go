package test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/antgobar/kvstore/stores/blt"
)

func setupBoltDb(t *testing.T) *blt.BltStore {
	suffix := strconv.Itoa(rand.Int())
	store, err := blt.New("TestDb"+suffix, "testUserBucket"+suffix, time.Second*1)
	if err != nil {
		t.Fatalf("Error setting up bolt store: %v", err)
	}
	return store
}
func tearDownBoltDb(t *testing.T, db *blt.BltStore) {
	err := db.TearDown()
	if err != nil {
		t.Fatalf("Error tearing down bolt store: %v", err)
	}
}
