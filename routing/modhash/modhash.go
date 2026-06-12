package modhash

import (
	"hash/fnv"
)

type ModHashRouter[T any] struct {
	shards []T
}

type Router[T any] interface {
	Route(key string) (T, error)
}

func NewModHashRouter[T any](shards []T) *ModHashRouter[T] {
	return &ModHashRouter[T]{shards: shards}
}

func (m *ModHashRouter[T]) Route(key string) (*T, error) {
	h := fnv.New64()
	h.Write([]byte(key))
	n := int(h.Sum64() % uint64(len(m.shards)))
	return &m.shards[n], nil
}
