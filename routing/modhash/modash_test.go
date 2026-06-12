package modhash

import (
	"slices"
	"testing"
)

func TestModHashRoutingConsistentRoute(t *testing.T) {
	shards := []string{"bucket1", "bucket2", "bucket3"}

	router := NewModHashRouter(shards)
	want := "foot"
	firstRoute, _ := router.Route(want)

	if got, _ := router.Route(want); got != firstRoute {
		t.Fatalf("non-deterministic: got %q, want %q", *got, *firstRoute)
	}
	if !slices.Contains(shards, *firstRoute) {
		t.Fatalf("invalid shard routed: got %q, expected one of %v", *firstRoute, shards)
	}
}
