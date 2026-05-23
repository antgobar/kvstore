# Go Distributed KV Store Library

A Go **library** that provides composable, pluggable building blocks for distributed key-value stores. Users wire together provided implementations or supply their own via minimal interfaces.

---

## Design Philosophy

- **Interface-driven** — core operations defined as minimal Go interfaces
- **Pluggable** — storage, transport, routing, replication, auth all swappable independently
- **Composable** — single node, sharded, multi-node cluster are different compositions of the same parts
- **Batteries included** — reference implementations for all common cases
- **Incremental** — simple single-node works first, complexity layered on top
- **No lock-in** — a Redis-backed storage only needs `Get`, `Put`, `Delete` (and `Scan` where needed)
- **Learning-oriented** — where external libraries exist (bbolt, hashicorp/raft), the library also provides a from-scratch implementation
- **Data/control plane separation** — user KV data and cluster metadata are stored and managed independently

### Data plane vs control plane

| Plane | What it stores | Sharded? | Moved on redistribution? |
| ----- | -------------- | -------- | ------------------------ |
| **Data plane** | User keys/values via `Storage` | Yes (hash ring) | Yes — keys transfer node-to-node |
| **Control plane** | Transfer state, node identity, credentials | No | No — stays local or in dedicated cluster backend |

**Invariant**: shard redistribution must never migrate control-plane state through the user-data transfer protocol. Transfer checkpoints live in `MetaStore`, not in sharded `Storage`.

---

## MVP Scope

| In MVP | Deferred |
| ------ | -------- |
| gRPC internal + HTTP/gRPC client-facing | Coordinated cluster-wide Scan |
| Consistent hash ring + peer forwarding | Merkle root cutover verification |
| Static + in-memory dynamic membership | HLC timestamps |
| Shard transfer + join/leave/failure | Static API key auth (Phase 7) |
| Leaderless quorum replication | JWT, RBAC |
| mTLS on internal cluster gRPC (Phase 4) | Gossip and Raft membership |
| `Entry` envelope for TTL and versioning | Distributed transactions |
| LBConfigSync for scaling | CASStorage implementations (Phase 7) |

---

## Deployment Topologies

| Mode | Client entry point | When to use |
| ---- | ------------------ | ----------- |
| **Dumb LB + peer forwarding** | nginx/HAProxy round-robin | Simple ops, curl-friendly |
| **Smart gateway** | Dedicated gateway tier | Central auth, rate limits, no forwarding hops |
| **Smart client** | SDK with embedded `Router` | Lowest latency, no gateway SPOF |

**LB pool timing during node transitions:**

| Node state | In LB pool? | Receives client traffic? |
| ---------- | ----------- | ------------------------ |
| `Receiving` | No | No — internal transfer only |
| `Ready` / `Active` | Yes | Yes |
| `TransferringOut` | Yes (draining) | Yes, forwards owned keys; new writes go to successor |

---

## Core Conventions

- **Values are `[]byte`**, keys are `string` — users serialize; library never interprets
- **Every method takes `context.Context` first**
- **Library-defined errors** — `ErrKeyNotFound`, `ErrThrottled`, `ErrNotOwner`, `ErrNodeUnavailable`, `ErrTransferInProgress`, `ErrUnauthorized`, `ErrInvalidArgument`
- **Ports are configurable** — client-facing and internal cluster on separate ports
- **`slog` for logging** — stdlib only; every component accepts optional `*slog.Logger`
- **Key/value size limits** configurable (defaults: key 1 KB, value 1 MB)

---

## Internal Storage Format — Entry Envelope

All storage implementations wrap user values in an `Entry` envelope — invisible to transport and routing layers.

```go
type Entry struct {
    Value     []byte
    ExpiresAt int64  // unix nano; 0 = no expiry
    Version   uint64 // incremented on every write; used for CAS and replication LWW
}
```

`ExpiresAt` is checked on every `Get` — expired entries return `ErrKeyNotFound`. A background sweeper per shard handles proactive cleanup. `TTLStorage.PutWithTTL` is a convenience wrapper that converts `time.Duration` to `Entry.ExpiresAt`.

---

## Package Structure

```
kvstore/
  storage/     — Storage, TTLStorage, CASStorage, ScanStorage + impls
  meta/        — MetaStore + impls (in-memory, bbolt)
  transport/   — TransportServer, TransportClient + HTTP/gRPC impls
  cluster/     — NodePool, hash ring, shard map, Membership, LBConfigSync
  replication/ — Replicator + leaderless quorum + leader-based
  node/        — wires all layers; NodeRole, NodeConfig
  gateway/     — smart gateway (Phase 5b)
  client/      — cluster-aware Client facade
  security/    — Authenticator, Authorizer, TLS helpers
  proto/       — protobuf definitions
  observe/     — Metrics, State snapshots
  testing/     — mock implementations of all interfaces
```

---

## Interface Layers

### Storage (data plane)

```go
type Storage interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
}

// Required for shard transfer
type ScanStorage interface {
    Storage
    Scan(ctx context.Context, fn func(key string, value []byte) error) error
}

// Optional — backends that support TTL natively
type TTLStorage interface {
    Storage
    PutWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// Optional — backends that support atomic operations (implementations in Phase 7)
type CASStorage interface {
    Storage
    CompareAndSwap(ctx context.Context, key string, oldVal, newVal []byte) (bool, error)
}
```

Reference implementations: in-memory map (all three), bbolt (Storage + ScanStorage), hand-rolled WAL (learning path).

### Control Plane Storage (MetaStore)

Operational metadata — not sharded, not redistributed.

```go
type MetaStore interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
    Scan(ctx context.Context, prefix string, fn func(key string, value []byte) error) error
}
```

Reference implementations: in-memory map (dev), bbolt (`meta.db` alongside user data).

**Three tiers of state:**

| Tier | Store | Consistency | Examples |
| ---- | ----- | ----------- | -------- |
| **User data** | `Storage` (sharded) | Per-key owner / quorum | User keys |
| **Local control** | `MetaStore` (per-node) | Local durability | Transfer checkpoints, hints, credentials |
| **Cluster coordination** | `Membership` backend | Strongly consistent | Node registry, liveness, ring generation |

**Why etcd:** membership decisions require strong consistency — split membership views during transfer produce split-brain. etcd provides watch/lease semantics that drive `MembershipEvent` notifications.

MetaStore key namespace: `node/` identity, `transfer/` checkpoints, `hints/` hinted handoff, `auth/` credentials. User keys **never** share a `MetaStore`.

### Transport

```go
type TransportServer interface {
    Serve(ctx context.Context, handler RequestHandler) error
    Shutdown(ctx context.Context) error
}

type TransportClient interface {
    Get(ctx context.Context, addr, key string) ([]byte, error)
    Put(ctx context.Context, addr, key string, value []byte) error
    Delete(ctx context.Context, addr, key string) error
}
```

Reference implementations: HTTP and gRPC. `TransportClient` is low-level — application code uses the `Client` facade.

### Routing

`Router.Route(key string) (addr string, err error)` — reference implementations: passthrough (single node), consistent hash ring (multi-node default), static shard map.

### Consistency & Conflict Resolution

| Topology | Replication | Guarantee | Conflict resolution |
| -------- | ----------- | --------- | ------------------- |
| Single node | none | linearizable | n/a |
| Sharded, no replication | none | per-key owner authoritative | n/a |
| Leaderless quorum | N/W/R | `R+W > N` → read-your-writes | LWW via `(Entry.Version, NodeID)` |
| Leader-based (Raft) | per-shard leader | linearizable per shard | Raft log ordering |

### Replication

```go
type Replicator interface {
    Write(ctx context.Context, op Operation) error
    Read(ctx context.Context, key string) ([]byte, error)
}

type Operation struct {
    Type    OpType // OpPut, OpDelete
    Key     string
    Value   []byte
    Version uint64 // from Entry.Version; used for LWW conflict resolution
}
```

Reference implementations: leaderless quorum (configurable N, W, R), leader-based (Phase 7).

- **Hinted handoff** — replica unavailable: writes stored under `hints/` in `MetaStore`, delivered on recovery
- **Read repair** — stale reads trigger background repair to the latest version

### Cluster Membership

```go
type Membership interface {
    Members() []NodeInfo
    Join(ctx context.Context, node NodeInfo) error
    Leave(ctx context.Context, nodeID string) error
    Watch(ctx context.Context, fn func(event MembershipEvent)) error
}

type MembershipEvent struct {
    Type NodeEventType // NodeJoined, NodeLeft, NodeFailed
    Node NodeInfo
}
```

Reference implementations: static config (MVP), in-memory dynamic (Phase 5 testing, no etcd), etcd-backed (Phase 5 production), gossip/Raft (Phase 7).

### Security

```go
type Principal interface{ ID() string }

type Authenticator interface {
    Authenticate(ctx context.Context, token []byte) (Principal, error)
}

type Authorizer interface {
    Authorize(ctx context.Context, p Principal, op string, key string) error
}
```

Auth runs before routing and replication — gRPC interceptors and HTTP middleware. mTLS on internal cluster gRPC is mandatory from Phase 4 (library never generates certs — user provides `*tls.Config`). Reference implementations: noop (Phase 1), static API key via `MetaStore` `auth/` (Phase 7), JWT (Phase 7).

### NodePool

`NodePool.Get(ctx, addr) (NodeConn, error)` — one persistent gRPC connection per peer. Retryable: `Unavailable`, `ResourceExhausted`. Never retry: `NotFound`, `InvalidArgument`.

### Client

`Client` — `Get / Put / Delete` facade. Hides routing from application code. Uses `Router` + `NodePool` in smart client mode; relies on forwarding in dumb-LB mode.

---

## Node Architecture

```go
type NodeRole int // ClientFacing | StorageOnly | Gateway
```

| Role | Client port | Internal port | DataStore |
| ---- | ----------- | ------------- | --------- |
| `ClientFacing` | `KVService` | all cluster services | yes |
| `StorageOnly` | none | all cluster services | yes |
| `Gateway` | `KVService` | membership only | no |

```go
type NodeConfig struct {
    Role       NodeRole
    ID, Addr   string
    Logger     *slog.Logger
    DataStore  Storage       // user data plane
    MetaStore  MetaStore     // control plane — separate backing store
    Router     Router
    Pool       NodePool
    Membership Membership
    Replicator Replicator    // optional
    Auth       Authenticator
    Authz      Authorizer
    Metrics    Metrics       // optional; nil = noop
}
```

Internal components (transfer checkpoints, hinted handoff, dedup) are constructed from `MetaStore` — not on `NodeConfig`.

---

## Sharding Architecture

```
key → hash(key) → cluster token → hash ring → NodeID       (ownership)
key → hash(key) mod 256 → local shard index                 (concurrency)
```

**Cluster level** — consistent hash ring with 100–200 virtual nodes per physical node. Adding/removing a node remaps only the affected key range.

**Node level** — ~256 local shards, each with its own RWMutex and token bucket. Concurrency partitioning only — not independently moveable during redistribution.

---

## Cluster Redistribution

Redistribution moves **user keys only** via `ShardService`. Control-plane state stays in `MetaStore`.

**Node join:** D announces `StateReceiving` → predecessor A streams keys via `TransferShard` → double-write to both during transfer → checkpoint in `MetaStore` `transfer/` → D announces `StateReady` → `LBConfigSync` adds D to pool → A deletes transferred keys.

**Node leave:** remove from LB pool → drain in-flight → transfer ranges to successor → `Membership.Leave`.

**Node failure:** heartbeat timeout → `Membership` marks dead → successors initiate transfer from last checkpoint → node removed from LB pool.

| Failure | Behaviour |
| ------- | --------- |
| D crashes mid-transfer | Predecessor continues; transfer restarts from checkpoint |
| Predecessor crashes | D discards partial; new owner re-transfers |
| Concurrent joins on same range | `Membership` serializes via generation token |
| Write during transfer | Double-write; sequence numbers ensure ordering |

---

## LB Config Sync

`LBConfigSync` watches `Membership` and pushes updated backend lists via `LBConfigSink.Apply(ctx, []Backend)`. Reference sinks: file writer (nginx upstream block), stdout, noop.

---

## Spike Handling

In-process burst capacity (autoscaling responds in minutes — wrong tool for spikes):

| Spike duration | Tool |
| -------------- | ---- |
| Seconds | Per-shard token bucket |
| Minutes | Adaptive capacity — hot shards borrow from node-level pool |
| Hours+ | Add nodes (planned, off-peak) |

---

## Transactions and Batches

| Operation | Scope | Phase |
| --------- | ----- | ----- |
| `Put / Get / Delete` | single key | 1 |
| `CAS` (via `CASStorage`) | single key, optimistic | 7 |
| `Batch` | multi-key atomic, single node | post-MVP |
| Distributed transaction | cross-node | out of scope |

`Entry.Version` exists from Phase 1 — CAS needs no storage format change.

---

## Observability

```go
type Metrics interface {
    RecordOperation(op string, duration time.Duration, err error)
    RecordShardSize(shardID uint32, keys int)
    RecordReplication(success bool, duration time.Duration)
    RecordTransfer(shardID uint32, keys int, duration time.Duration)
}
```

Not coupled to any metrics library — wire in Prometheus or OpenTelemetry. Nil = noop.

Every component exposes `State()` returning a structured snapshot (node status, shard counts, token levels, peers, transfers, storage stats) via optional `/debug/state`. Shutdown: drain requests → checkpoint transfers → `Membership.Leave` → `TransportServer.Shutdown`.

---

## gRPC Service Definitions

```protobuf
// ClientFacing + Gateway roles
service KVService {
    rpc Get(GetRequest) returns (GetResponse);
    rpc Put(PutRequest) returns (PutResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc Scan(ScanRequest) returns (stream ScanResponse);
}

// ClientFacing + StorageOnly roles (internal cluster)
service ForwardService {
    rpc Forward(ForwardRequest) returns (ForwardResponse);
}

service ReplicationService {
    rpc Replicate(ReplicateRequest) returns (ReplicateResponse);
    rpc ReplicateStream(stream ReplicateRequest) returns (ReplicateResponse);
}

service ShardService {
    rpc TransferShard(stream ShardChunk) returns (TransferResponse);
    rpc ShardInfo(ShardInfoRequest) returns (ShardInfoResponse);
}

service ClusterService {
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
    rpc Join(JoinRequest) returns (JoinResponse);
    rpc Leave(LeaveRequest) returns (LeaveResponse);
    rpc ClusterState(ClusterStateRequest) returns (ClusterStateResponse);
}

// Phase 7: hand-rolled or hashicorp/raft — RaftService (RequestVote, AppendEntries)
```

---

## Incremental Build Plan

### Phase 1 — Foundation
- Core interfaces, error types, `Entry` type; in-memory `Storage` with TTL sweeper; in-memory `MetaStore`
- HTTP transport; single node works end to end; noop auth; `testing/` mocks

### Phase 2 — Persistence (two paths)
- **Path A — bbolt**: bbolt `DataStore` + bbolt `MetaStore` (`meta.db`); `ScanStorage` + `TTLStorage`
- **Path B — WAL**: hand-rolled append-only log, recovery, compaction (learning path)

### Phase 3 — Node-level sharding
- Local shard map (~256 shards), per-shard RWMutex + token bucket; adaptive capacity pool
- `Metrics` interface wired; `DedupStore` in `MetaStore`

### Phase 4 — Static multi-node + gRPC
- gRPC transport; `NodePool`; static `Membership`; consistent hash ring; `ForwardService`
- **mTLS on internal cluster gRPC — mandatory, not optional**; `NodeRole` config; `/debug/state`

### Phase 4b — Client + LB sync
- `Client` facade with retry; `LBConfigSync` + reference sinks (file writer, stdout, noop)

### Phase 5 — Dynamic cluster
- Join/leave/failure redistribution; `ShardService` streaming; transfer checkpoints in `MetaStore`
- In-memory dynamic `Membership` (testing); etcd-backed `Membership` (production)

### Phase 5b — Gateway + smart client
- `gateway/` package; `StorageOnly` nodes behind gateway; smart client SDK with embedded `Router`

### Phase 6 — Replication
- `ReplicationService`; leaderless quorum (N, W, R configurable); hinted handoff; read repair

### Phase 7 — Auth + advanced
- Static API key auth via `MetaStore` `auth/`; JWT; `CASStorage` implementations
- Leader-based replication; gossip + Raft membership (two paths each)

---

## Out of Scope (v1)

- Encryption at rest
- Backup/restore tooling
- Coordinated snapshot Scan across cluster
- Distributed transactions (2PC, SSI); cross-node multi-key atomicity
- Full RBAC / user management UI
- Replicating local `MetaStore` between nodes

---

## Open Design Questions

- Extension mechanism for richer APIs — e.g. Redis adapter exposing INCR or EXPIRE
- How to expose `ClusterState` to operators — HTTP only, gRPC reflection, or both
- Multi-gateway credential sharing — external IdP or shared store recommended

---

## Reference Material

- **Designing Data-Intensive Applications** — Kleppmann: Ch6, Ch7, Ch9, Ch10
- **Dynamo paper** (Amazon, 2007) — leaderless replication, consistent hashing, quorum
- **gRPC Go** — https://grpc.io/docs/languages/go/
- **bbolt** — https://github.com/etcd-io/bbolt
- **hashicorp/raft** — https://github.com/hashicorp/raft
- **Go slog** — https://pkg.go.dev/log/slog
