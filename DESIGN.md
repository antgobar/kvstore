# Go Distributed KV Store Library — Project Prompt

## What I Am Building

A Go **library** (not an application) that provides composable, pluggable building blocks for
distributed key-value stores. Users wire together provided implementations or supply their own
via interfaces. Everything works out of the box but nothing is locked in.

This is a plugin-style architecture. For example:

- I provide an HTTP server/client transport and a gRPC server/client transport
- I provide an in-memory map (with RWMutex) and a bbolt storage backend
- I provide a consistent hash ring router, a static shard router, a passthrough router
- Users can swap any of these out by implementing the relevant interface
- The interfaces are intentionally minimal to ensure generalisation across backends

---

## Design Philosophy

- **Interface-driven** — core operations defined as minimal Go interfaces
- **Pluggable** — storage, transport, routing, replication all swappable independently
- **Composable** — single node, sharded node, multi-node cluster are different compositions of the same parts
- **Batteries included** — reference implementations for all common cases
- **Incremental** — simple single-node works first, complexity layered on top
- **No lock-in** — a user providing a Redis-backed storage only needs to implement Get, Put, Delete (and Scan where needed)
- **Learning-oriented** — where external libraries exist (bbolt, hashicorp/raft), the library also provides a from-scratch implementation so the internals are understood. Users choose which to use.

---

## Core Conventions (apply everywhere)

- **Value type is `[]byte`** throughout — transport-agnostic, users serialize on top
- **Every interface method accepts `context.Context` as first argument** — no exceptions
- **Error types are defined by the library** — callers distinguish errors by type, not string matching:
  - `ErrKeyNotFound`
  - `ErrThrottled`
  - `ErrNotOwner`
  - `ErrNodeUnavailable`
  - `ErrTransferInProgress`
- **Ports are configurable** — the library never hardcodes port numbers, always accepts config. Convention (not requirement): client-facing on one port, internal cluster on another.
- **slog for logging** — stdlib since Go 1.21, no external dependency. Every component accepts an optional `*slog.Logger`. If nil, a no-op logger is used.

---

## Package Structure

```
kvstore/
  storage/        — Storage interface + all storage implementations
  transport/      — TransportServer, TransportClient interfaces + HTTP/gRPC implementations
  cluster/        — NodePool, hash ring, shard map, membership, state
  replication/    — replication strategy interface + implementations
  node/           — composes storage, transport, routing, replication into a working node
  proto/          — protobuf definitions for gRPC services
  observe/        — Metrics interface, StateInspector interface, structured state types
  testing/        — mock implementations of all interfaces for use in user tests
```

---

## Interface Layers

### Storage

```go
type Storage interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
}

// Extended — required for shard transfer and rebalancing
type ScanStorage interface {
    Storage
    Scan(ctx context.Context, fn func(key string, value []byte) error) error
}

// Optional — for backends that support TTL natively
type TTLStorage interface {
    Storage
    PutWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// Optional — for backends that support atomic operations
type CASStorage interface {
    Storage
    CompareAndSwap(ctx context.Context, key string, oldVal, newVal []byte) (bool, error)
}
```

Reference implementations:

- In-memory map with RWMutex (implements Storage + ScanStorage + TTLStorage)
- bbolt (implements Storage + ScanStorage) — uses bbolt's own durability
- Hand-rolled WAL-backed store (implements Storage + ScanStorage) — learning implementation, not dependent on bbolt

### Transport (Client-Facing)

```go
type TransportServer interface {
    Serve(ctx context.Context, handler RequestHandler) error
    Shutdown(ctx context.Context) error
}

type TransportClient interface {
    Get(ctx context.Context, addr string, key string) ([]byte, error)
    Put(ctx context.Context, addr string, key string, value []byte) error
    Delete(ctx context.Context, addr string, key string) error
}
```

Reference implementations: HTTP, gRPC. HTTP is valid for client-facing — easy to test with curl, good for CLIs.

### Cluster Transport (Internal Node-to-Node)

- **gRPC only** — HTTP is not suitable for internal traffic
- Reasons: native streaming for shard transfer, HTTP/2 multiplexing, typed protobuf contracts, bidirectional streaming for replication and gossip
- Users could theoretically swap but there is little practical reason to

### Routing

```go
type Router interface {
    // returns the node address responsible for this key
    Route(key string) (addr string, err error)
}
```

Reference implementations: passthrough (single node), consistent hash ring, static shard map.

### Replication

```go
type Replicator interface {
    Replicate(ctx context.Context, op Operation) error
}
```

Reference implementations: leaderless quorum, leader-based. Configurable N (replicas), W (write quorum), R (read quorum).

### Cluster Membership

```go
type Membership interface {
    Members() []NodeInfo
    Join(ctx context.Context, node NodeInfo) error
    Leave(ctx context.Context, nodeID string) error
    Watch(ctx context.Context, fn func(event MembershipEvent)) error
}
```

Reference implementations: static config, gossip (hand-rolled for learning), etcd-backed, raft-backed.

---

## Observability — First-Class Concern

Observability covers three things: metrics, logging, and state inspection. All three are needed — metrics tell you something is wrong, logging tells you what happened, state inspection tells you the current condition of any component.

### Logging

```go
// Every component accepts an optional logger
type NodeConfig struct {
    Logger *slog.Logger // nil = no-op
    // ...
}
```

Structured fields on every log entry: `node_id`, `shard_id`, `operation`, `key` (hashed, not raw), `duration_ms`, `error`.

### Metrics

```go
// Optional interface — users provide their own implementation
// e.g. backed by Prometheus, OpenTelemetry, or a simple in-memory counter
type Metrics interface {
    RecordOperation(op string, duration time.Duration, err error)
    RecordShardSize(shardID uint32, keys int)
    RecordTokenBucket(shardID uint32, tokens float64)
    RecordReplication(success bool, duration time.Duration)
    RecordTransfer(shardID uint32, keys int, duration time.Duration)
}
```

Not coupled to any specific metrics library. User wires in Prometheus, OpenTelemetry, or anything else.

### State Inspection

This is critical for understanding live node, store and cluster state. Every major component exposes a `State()` method returning a structured, serialisable snapshot:

```go
// Node-level state
type NodeState struct {
    ID          string
    Addr        string
    Status      NodeStatus  // Active, Receiving, Transferring
    ShardCount  int
    Shards      []ShardState
    PeerCount   int
    Peers       []PeerState
}

// Per-shard state
type ShardState struct {
    ID          uint32
    KeyCount    int
    Tokens      float64     // current token bucket level
    Status      ShardStatus // Active, Transferring, Receiving
    OwnerNode   string
}

// Cluster-level state
type ClusterState struct {
    Nodes       []NodeState
    TotalKeys   int
    TotalShards int
    RingHash    uint32      // fingerprint of current ring config
}

// Storage-level state
type StorageState struct {
    Implementation string
    KeyCount       int
    SizeBytes      int64
    ShardStates    []ShardState
}
```

These are exposed via:

- A `StateInspector` interface any component can implement
- An optional HTTP debug endpoint (e.g. `/debug/state`) on the client-facing server
- Loggable on demand via slog

```go
type StateInspector interface {
    State(ctx context.Context) (any, error)
}
```

---

## gRPC Service Definitions

### Client-Facing (configurable port, default convention)

```protobuf
service KVService {
    rpc Get(GetRequest) returns (GetResponse);
    rpc Put(PutRequest) returns (PutResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc Scan(ScanRequest) returns (stream ScanResponse);
}
```

### Internal Cluster (separate configurable port)

```protobuf
// Request forwarding — when a node receives a request it doesn't own
// Kept separate from KVService to carry forwarding metadata (hop count, origin node)
// x-forwarded metadata prevents infinite forwarding loops
service ForwardService {
    rpc Forward(ForwardRequest) returns (ForwardResponse);
}

// Write propagation to replicas
service ReplicationService {
    rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
    rpc ReplicateStream(stream ReplicateRequest) returns (ReplicateResponse);
}

// Shard ownership and transfer
service ShardService {
    rpc TransferShard(stream ShardChunk) returns (TransferResponse);
    rpc ShardInfo(ShardInfoRequest) returns (ShardInfoResponse);
}

// Cluster membership and liveness
service ClusterService {
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc Join(JoinRequest) returns (JoinResponse);
    rpc Leave(LeaveRequest) returns (LeaveResponse);
    rpc ClusterState(ClusterStateRequest) returns (ClusterStateResponse);
}

// Leader election
// Two paths:
// (a) Hand-rolled implementation using this proto — for learning
// (b) Delegate entirely to hashicorp/raft which owns its own transport — for production
service RaftService {
    rpc RequestVote(VoteRequest) returns (VoteResponse);
    rpc AppendEntries(AppendRequest) returns (AppendResponse);
}
```

---

## Node Architecture

Every node is simultaneously:

- A gRPC **server** on two configurable ports (client-facing, internal cluster)
- A gRPC **client** maintaining persistent connections to all peer nodes via a `NodePool`

Connection rules:

- One persistent connection per peer, created at startup, reused for all RPCs
- Never open/close connections per request
- gRPC handles reconnection automatically on transport failure — just retry the RPC
- Keepalive config must match on both client and server sides or connections drop silently
- Retryable error codes: `Unavailable`, `ResourceExhausted` — never retry `NotFound`, `InvalidArgument`

---

## Sharding Architecture

Two independent levels:

**Cluster level** — consistent hash ring maps keys to nodes

- Virtual nodes (100-200 per physical node) smooth uneven distribution
- Adding/removing a node only remaps keys in the affected range

**Node level** — each node maintains a shard map (~256 shards)

- Each shard has its own RWMutex — concurrent ops on different key ranges don't block each other
- Each shard has its own token bucket for burst capacity
- Shards are first-class moveable units — same transfer mechanism for node joins and intra-cluster rebalancing
- Powers of 2 for shard count makes modulo a cheap bitwise op

Key lookup path:

```
key → hash → ShardID → NodeID
```

---

## Node Join / Shard Transfer Protocol

When a new node D joins and claims a range currently owned by node A:

1. D announces `StateReceiving` to the cluster
2. A identifies keys in D's range by scanning its local store
3. A streams those keys to D via `TransferShard` (streaming gRPC)
4. During transfer: **writes go to both A and D**, reads go to A only
5. D announces `StateReady`
6. Cluster switches: all reads and writes now go to D
7. A deletes its copy of the transferred keys

The double-write window ensures writes during transfer are not lost — D is fully caught up by the time the bulk transfer finishes.

Forwarded requests carry an `x-forwarded` metadata flag to prevent infinite forwarding loops.

---

## Spike Handling vs Node Autoscaling

Node autoscaling is **not** suitable for traffic spikes:

- DynamoDB's own docs state autoscaling only triggers after 2+ minutes of sustained high load
- Total end-to-end latency from spike to new capacity is ~3-5 minutes
- By then the spike is over

Correct tools for spikes (all in-process, zero latency):

**Token bucket per shard** — accumulates up to 5 minutes of unused capacity (DynamoDB's burst capacity model). During a spike, stored tokens absorb the load without throttling.

**Adaptive capacity** — hot shards can borrow from a global node-level token pool when their own bucket is exhausted, provided total node capacity is not exceeded.

**Node autoscaling** is for **sustained growth** only — dataset outgrows memory, weeks of elevated traffic, capacity planning.

| Spike duration | Right tool                      |
| -------------- | ------------------------------- |
| Seconds        | Per-shard token bucket          |
| Minutes        | Adaptive capacity (global pool) |
| Hours+         | Add nodes (planned, off-peak)   |

---

## Incremental Build Plan

### Phase 1 — Foundation

- Define core interfaces: `Storage`, `TransportServer`, `TransportClient`
- Define core error types
- In-memory map storage implementation
- HTTP transport implementation
- Single node, no distribution, works end to end
- `slog` logging wired into every component from day one
- `StorageState` and `NodeState` structs + `State()` methods on all components
- `testing/` package: mock Storage, mock TransportClient

### Phase 2 — Persistence (two paths, user chooses)

- **Path A — bbolt**: bbolt storage implementation, bbolt handles durability internally, no WAL needed
- **Path B — hand-rolled WAL**: implement append-only WAL log, recovery on startup, compaction — learning implementation to understand how bbolt works under the hood
- `Scan` added to storage interface (required by both paths)
- `StorageState.SizeBytes` populated

### Phase 3 — Node-level Sharding

- Shard map within a single node (~256 shards)
- Per-shard RWMutex
- Per-shard token bucket (burst capacity)
- Global node-level token pool (adaptive capacity)
- `Router` interface introduced (passthrough initially)
- `ShardState` fully populated, token bucket level observable via state
- `Metrics` interface introduced, wired into shard operations

### Phase 4 — Static Multi-Node

- gRPC transport implementation
- `NodePool` — persistent connections to peers
- Static cluster config
- Consistent hash ring router
- Request forwarding (`ForwardService`) when node doesn't own a key
- `KVService` and `ForwardService` protos
- `PeerState` populated in `NodeState`
- Optional `/debug/state` HTTP endpoint on client-facing server

### Phase 5 — Dynamic Cluster

- Node join/leave protocol
- Shard transfer streaming (`ShardService`)
- Double-write window during transfers
- `ClusterService` proto
- `ClusterState` fully observable — all node states, shard ownership, ring fingerprint
- Transfer progress logged and exposed via state inspection

### Phase 6 — Replication

- `ReplicationService` proto
- Leaderless quorum implementation
- Configurable N, R, W
- Read repair
- Replication lag observable via `NodeState`

### Phase 7 — Advanced (two paths per item, user chooses)

- **Gossip membership**:
  - Path A: hand-rolled gossip protocol (learning)
  - Path B: use existing gossip library
- **Raft consensus**:
  - Path A: hand-rolled Raft implementation using `RaftService` proto (learning)
  - Path B: `hashicorp/raft` which owns its own transport (production)
- Adaptive capacity at cluster level
- Leader-based replication

---

## gRPC Services by Phase

| Phase   | Services                                        |
| ------- | ----------------------------------------------- |
| Phase 4 | `KVService`, `ForwardService`                   |
| Phase 5 | + `ShardService`, `ClusterService`              |
| Phase 6 | + `ReplicationService`                          |
| Phase 7 | + `RaftService` (or delegate to hashicorp/raft) |

---

## CLI Client Pattern

For any CLI tooling built on top of the library:

- One connection per process invocation (not per request)
- Always set `context.WithTimeout` — never hang on a dead server
- Use `os.Exit(1)` on errors for script composability
- Config priority: flag > env var > config file (kubectl/redis-cli pattern)
- No keepalives needed — connection is too short-lived to matter
- If adding a REPL/shell mode: dial once on shell start, reuse for all commands

---

## Open Design Questions

- Extension mechanism for users who need richer APIs than the base interface provides — e.g. a Redis adapter that wants to expose INCR or EXPIRE
- Whether cluster membership is itself a fully pluggable interface or partially opinionated (static config is clearly pluggable, but gossip vs raft changes the consistency guarantees significantly)
- How to expose `ClusterState` to external operators — HTTP endpoint, gRPC reflection, both?
- CAS and TTL — confirm as optional extended interfaces rather than core, decide which phases they land in

---

## Reference Material

- **Designing Data-Intensive Applications** — Kleppmann (2nd ed)
  - Chapter 6: Replication
  - Chapter 7: Partitioning
  - Chapter 9: The Trouble with Distributed Systems
  - Chapter 10: Consistency and Consensus
- **Dynamo paper** (Amazon, 2007) — canonical reference for leaderless replication, consistent hashing, shard transfer, quorum
- **gRPC Go docs** — https://grpc.io/docs/languages/go/
- **bbolt** — https://github.com/etcd-io/bbolt
- **hashicorp/raft** — https://github.com/hashicorp/raft
- **Go slog docs** — https://pkg.go.dev/log/slog
