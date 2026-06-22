# kvstore

A Go library of composable building blocks for building your own key-value store. The longer-term goal is a leaderless distributed store, assembled from small, swappable parts.

## The idea

The library is interface-driven. The core operations (`Get`, `Set`, `Delete`) are defined as small Go interfaces, and everything else is built on top.

You pick the pieces you want:

- A backing store: the in-memory map or the bbolt-backed store that ship with the library.
- A transport: HTTP or gRPC.
- Routing: a hash router for spreading keys across shards (early, see below).

Or you supply your own. If you want a Redis-backed store, you implement the `core.Store` interface and pass it to any transport. The library never interprets your values, so you are free to store whatever you want.

The full design, including the planned distributed and leaderless pieces, lives in [design.md](design.md). That document describes where the project is heading and is ahead of the current code.

## Project status

This is early. What works today is the single-node path: a store behind an HTTP or gRPC server, with a matching client and a small CLI. Sharding, multi-node clusters, and replication are described in [design.md](design.md) but are not built yet. Treat the interfaces as still moving.

## Install

```bash
go get github.com/antgobar/kvstore
```

Requires Go 1.26 or newer.

## Core concepts

Keys are strings, values are `[]byte`. The library does not serialize or interpret values for you. Every method takes a `context.Context` first.

The central interface is `core.Store`:

```go
type Store interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Set(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
}
```

Stores that can iterate keys also implement `core.Scanner`, and the two together form `core.ScanStore`:

```go
type Scanner interface {
    Scan(ctx context.Context, prefix string) (<-chan []map[string][]byte, <-chan error)
}

type ScanStore interface {
    Store
    Scanner
}
```

A missing key returns `core.ErrKeyNotFound`.

## Quick start (HTTP)

Run a server backed by the in-memory store:

```go
package main

import (
    "time"

    store "github.com/antgobar/kvstore/stores/memory"
    "github.com/antgobar/kvstore/transport/http/server"
)

func main() {
    s := store.New()
    httpServer := server.New("localhost:8080", s, 10*time.Second)
    httpServer.Run()
}
```

The server exposes three JSON endpoints, all `POST`:

- `POST /set` with body `{"key": "...", "value": "..."}`
- `POST /get` with body `{"key": "..."}`, returns `{"value": "..."}`
- `POST /delete` with body `{"key": "..."}`

Talk to it from Go with the HTTP client and the CLI helper:

```go
package main

import (
    "time"

    "github.com/antgobar/kvstore/cli"
    "github.com/antgobar/kvstore/transport/http/client"
)

func main() {
    c := client.New("http://localhost:8080", 10*time.Second)
    cli.Run(c)
}
```

`cli.Run` reads flags from the command line:

```bash
# set a key
go run ./examples/http_map/client -a set -k foo -v bar

# read it back
go run ./examples/http_map/client -a get -k foo

# delete it
go run ./examples/http_map/client -a delete -k foo
```

The runnable versions of both programs are in [examples/http_map](examples/http_map).

## Quick start (gRPC)

The gRPC path follows the same shape. Server:

```go
package main

import (
    "time"

    store "github.com/antgobar/kvstore/stores/memory"
    "github.com/antgobar/kvstore/transport/grpc/server"
)

func main() {
    s := store.New()
    grpcServer := server.NewGrpcServer("localhost:50051", s, 10*time.Second)
    grpcServer.Run()
}
```

Client:

```go
package main

import (
    "time"

    "github.com/antgobar/kvstore/cli"
    "github.com/antgobar/kvstore/transport/grpc/client"
)

func main() {
    c := client.New("localhost:50051", 10*time.Second)
    cli.Run(c)
}
```

The same `-a`, `-k`, `-v` flags apply. See [examples/grpc_map](examples/grpc_map).

## Bring your own store

Any type that satisfies `core.Store` can sit behind either transport. A Redis-backed store, sketched out, looks like this:

```go
package redisstore

import (
    "context"

    "github.com/antgobar/kvstore/core"
    "github.com/redis/go-redis/v9"
)

type RedisStore struct {
    client *redis.Client
}

func (r *RedisStore) Get(ctx context.Context, key string) ([]byte, error) {
    v, err := r.client.Get(ctx, key).Bytes()
    if err == redis.Nil {
        return nil, core.ErrKeyNotFound
    }
    return v, err
}

func (r *RedisStore) Set(ctx context.Context, key string, value []byte) error {
    return r.client.Set(ctx, key, value, 0).Err()
}

func (r *RedisStore) Delete(ctx context.Context, key string) error {
    return r.client.Del(ctx, key).Err()
}
```

Return `core.ErrKeyNotFound` for missing keys so the transports map it to the right status code. Pass an instance to `server.New(...)` or `server.NewGrpcServer(...)` and it works with the existing clients and CLI.

## Choosing a backing store

Two stores ship with the library:

- `stores/memory` ([stores/memory/memory.go](stores/memory/memory.go)): an in-memory map guarded by a mutex. Good for development and tests. Data is lost on restart.
- `stores/blt` ([stores/blt/blt.go](stores/blt/blt.go)): backed by [bbolt](https://github.com/etcd-io/bbolt) for on-disk persistence. Construct it with `blt.New(storeName, userSpaceName, timeout)`.

Both implement `core.Store` and `core.Scanner`.

## Routing (preview)

`routing/modhash` ([routing/modhash/modhash.go](routing/modhash/modhash.go)) holds a generic hash router that maps a key to one of N shards:

```go
router := modhash.NewModHashRouter([]core.Store{storeA, storeB, storeC})
shard, err := router.Route("some-key")
```

This is groundwork for spreading keys across stores or nodes. It is not wired into the transports yet.

## Project layout

- `core/`: shared types, the `Store` interface, and error values.
- `stores/memory`, `stores/blt`: store implementations.
- `transport/http`, `transport/grpc`: server and client per transport.
- `cli/`: small command-line runner over any client.
- `routing/modhash`: hash router (preview).
- `server/`: the `Server` interface (`Run`/`Stop`).
- `examples/`: runnable HTTP and gRPC programs.
- `internal/genproto`: generated protobuf code.
- `test/`: integration tests across transports.

## Development

Common tasks are in the [Makefile](Makefile):

```bash
make test                        # run all tests
make format                      # go fmt
make build                       # build the example binaries
make build-run-http-map-example  # build and run the HTTP example server
make build-run-grpc-map-example  # build and run the gRPC example server
make gen-proto                   # regenerate protobuf code
```

## Roadmap

The phased plan in [design.md](design.md) covers, roughly in order:

- Persistence beyond the in-memory store (bbolt, and a hand-rolled write-ahead log as a learning path).
- Node-level sharding with per-shard locking.
- Multi-node clusters over gRPC with a consistent hash ring and request forwarding.
- Dynamic membership: join, leave, and failure handling with shard transfer.
- Leaderless quorum replication with configurable N, W, and R.
- Authentication and authorization.

## References

- Dynamo paper (Amazon, 2007): leaderless replication, consistent hashing, quorums.
- Designing Data-Intensive Applications, Martin Kleppmann.
- bbolt: https://github.com/etcd-io/bbolt
- gRPC for Go: https://grpc.io/docs/languages/go/
