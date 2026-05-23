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
- **Pluggable** — storage, transport, routing, replication, auth all swappable independently
- **Composable** — single node, sharded node, multi-node cluster are different compositions of the same parts
- **Batteries included** — reference implementations for all common cases
- **Incremental** — simple single-node works first, complexity layered on top
- **No lock-in** — a user providing a Redis-backed storage only needs to implement Get, Put, Delete (and Scan where needed)
- **Learning-oriented** — where external libraries exist (bbolt, hashicorp/raft), the library also provides a from-scratch implementation so the internals are understood. Users choose which to use.

---

## Deployment Topologies

Three first-class deployment modes, all composable from the same parts:

```mermaid
flowchart LR
  subgraph dumbLB [DumbLB_PeerForwarding]
    Client1[Client] --> Nginx[Nginx_RR]
    Nginx --> NodeA[NodeA]
    Nginx --> NodeB[NodeB]
    NodeA -->|"ForwardService if not owner"| NodeB
  end

  subgraph smartGW [SmartGateway]
    Client2[Client] --> GW[Gateway]
    GW -->|"Router.Route key"| NodeC[NodeC]
    GW --> NodeD[NodeD]
  end

  subgraph smartClient [SmartClient]
    Client3[ClientSDK] -->|"Router.Route key"| NodeE[NodeE]
    Client3 --> NodeF[NodeF]
  end
```

| Mode | Client entry point | Routing knowledge | When to use |
| ---- | ------------------ | ----------------- | ----------- |
| **Dumb LB + peer forwarding** | nginx/HAProxy round-robin | None on client or LB; any node may receive request | Simple ops, curl-friendly, minimal client logic |
| **Smart gateway** | Dedicated gateway tier | Gateway holds `Router` + `Membership` | Central auth, rate limits, no forwarding hops |
| **Smart client** | SDK with embedded `Router` | Client holds ring; talks directly to owner | Lowest latency, no gateway SPOF |

### Dumb LB + peer forwarding

- Storage nodes remain client-facing (existing `KVService` port)
- LB has **no hash-ring awareness** — round-robin only
- Nodes use `ForwardService` when they don't own the key
- **`LBConfigSync`** watches `Membership.Watch()` and emits updated upstream lists (nginx config snippet, HAProxy backend list, or K8s Endpoints patch). Pluggable sink interface:

```go
type LBConfigSink interface {
    Apply(ctx context.Context, backends []Backend) error
}
```

Reference sinks: file writer (nginx `upstream` block), stdout (for sidecar), noop.

### Smart gateway

- Gateway composes `Router`, `Membership`, `NodePool`, optional auth
- Storage nodes may run **internal port only** (no client-facing) in this mode
- Gateway exposes `KVService`; no `ForwardService` on the hot path

### Smart client

- Client SDK embeds `Router` and `NodePool`
- Talks directly to the owning node — no forwarding hop, no gateway tier

### Node roles

Every process has a role that determines which services bind to which ports:

```go
type NodeRole int // StorageOnly | ClientFacing | Gateway
```

| Role | Client-facing port | Internal port | Notes |
| ---- | ------------------ | ------------- | ----- |
| `ClientFacing` | `KVService` | cluster services | Default for dumb-LB mode |
| `StorageOnly` | none | cluster services | Used behind smart gateway |
| `Gateway` | `KVService` | optional (membership only) | No local storage |

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
  - `ErrUnauthorized`
  - `ErrForbidden`
  - `ErrInvalidArgument`
- **Idempotency** — all mutating ops accept an optional `RequestID`; safe to retry on `Unavailable`
- **Request ID** — propagated through the forward chain to prevent duplicate side effects
- **Limits** — configurable max key size (default 1 KB), max value size (default 1 MB); violations return `ErrInvalidArgument`
- **Ports are configurable** — the library never hardcodes port numbers, always accepts config. Convention (not requirement): client-facing on one port, internal cluster on another.
- **slog for logging** — stdlib since Go 1.21, no external dependency. Every component accepts an optional `*slog.Logger`. If nil, a no-op logger is used.

---

## Package Structure

```
kvstore/
  storage/        — Storage interface + all storage implementations
  transport/      — TransportServer, TransportClient interfaces + HTTP/gRPC implementations
  cluster/        — NodePool, hash ring, shard map, membership, LBConfigSync
  replication/    — replication strategy interface + implementations
  node/           — composes storage, transport, routing, replication; NodeRole config
  gateway/        — smart gateway composition
  client/         — cluster-aware Client facade
  security/       — Authenticator, Authorizer, TLS helpers
  proto/          — protobuf definitions for gRPC services
  observe/        — Metrics, Tracer, StateInspector, structured state types
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

Encryption at rest is backend-dependent and out of scope for the library core — users configure it on their chosen storage backend if supported.

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

`TransportClient` is low-level (address per call) — used by gateway and internal components. Application code should use the cluster-aware `Client` interface instead.

### Cluster Transport (Internal Node-to-Node)

- **gRPC only** — HTTP is not suitable for internal traffic
- Reasons: native streaming for shard transfer, HTTP/2 multiplexing, typed protobuf contracts, bidirectional streaming for replication and gossip
- Users could theoretically swap but there is little practical reason to

### Security

Pluggable authentication and authorization. Auth runs **before** routing and replication.

```go
type Principal interface {
    ID() string
    // opaque claims/metadata
}

type Authenticator interface {
    Authenticate(ctx context.Context, token []byte) (Principal, error)
}

type Authorizer interface {
    Authorize(ctx context.Context, p Principal, op string, key string) error
}
```

Wire points:

- gRPC: unary + stream interceptors on `TransportServer` and gateway
- HTTP: middleware wrapper around `RequestHandler`
- Internal cluster: optional mTLS-as-identity (`Principal` derived from client cert CN/SAN)

Reference implementations: **noop** (default), **static API key**, **JWT** (optional dependency).

Gateway mode is the natural place for centralized auth. In dumb-LB mode, each node runs the same `Authenticator`/`Authorizer` chain.

TLS configuration:

```go
type TLSConfig struct {
    Config *tls.Config // user-provided; library never generates certs
}
```

Accepted by all servers and `NodePool` dial options:

- Server TLS (client-facing)
- mTLS (internal cluster — recommended default for multi-node)
- Cert rotation is user responsibility; library accepts `*tls.Config`

### Routing

```go
type Router interface {
    // returns the node address responsible for this key
    Route(key string) (addr string, err error)
}
```

Reference implementations: passthrough (single node), consistent hash ring, static shard map (predefined key-range → node map at cluster level).

### Consistency & Conflict Resolution

The `Membership` interface is fully pluggable, but each backend carries different consistency guarantees. Choose topology and replication together — they are not independent drop-ins.

| Topology | Replication | Caller guarantee | Conflict resolution |
| -------- | ----------- | ---------------- | ------------------- |
| Single node, no replication | none | linearizable (single writer) | n/a |
| Sharded, no replication | none | per-key owner is authoritative | n/a |
| Leaderless quorum | N/W/R | tunable: `R+W > N` → read-your-writes possible | **LWW** via `(HLC timestamp, node_id)` tuple on every write |
| Leader-based (Raft) | per-shard leader | linearizable per shard | Raft log ordering |

Shared write descriptor used by replication and transfer:

```go
type Operation struct {
    Type      OpType // Put, Delete
    Key       string
    Value     []byte // nil for Delete
    Version   Version
    RequestID string // idempotency key for retries
}

type Version struct {
    Timestamp time.Time // or uint64 HLC
    NodeID    string
}
```

Split-brain handling depends on membership backend:

- **Static config** — manual operator intervention
- **Gossip** — version vectors detect divergence
- **Raft** — prevented by consensus protocol

`ErrNotOwner` is returned only when forwarding is disabled or hop limit is exceeded — not to external clients in dumb-LB or gateway modes (requests are forwarded or routed transparently).

### Replication

```go
type Replicator interface {
    Write(ctx context.Context, op Operation) error        // quorum write
    Read(ctx context.Context, key string) ([]byte, error) // quorum read
}

type HintStore interface {
    PutHint(ctx context.Context, key string, targetNode string) error
    GetHints(ctx context.Context, nodeID string) ([]Hint, error)
}
```

Reference implementations: leaderless quorum, leader-based. Configurable N (replicas), W (write quorum), R (read quorum).

- **Hinted handoff** — when a replica node is temporarily unavailable, writes are stored as hints and delivered on recovery
- **Read repair** — stale reads trigger background repair to the latest version
- **Idempotency** — replicas deduplicate by `RequestID` on retry

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

Each backend is a valid `Membership` implementation, but operators must understand the consistency trade-offs documented in the matrix above.

### NodePool

Persistent connections to peer nodes. Referenced throughout cluster operations but defined here:

```go
type NodePool interface {
    Get(ctx context.Context, addr string) (NodeConn, error)
    Close() error
}

type NodeConn interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
    Forward(ctx context.Context, req ForwardRequest) (ForwardResponse, error)
    Addr() string
}
```

Rules:

- One persistent gRPC connection per peer, created at startup, reused for all RPCs
- Keepalive config must match on both client and server sides
- Optional circuit breaker wrapper for `ErrNodeUnavailable` backoff

### Client API

Cluster-aware client — hides address routing from application code:

```go
type Client interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
    Scan(ctx context.Context, opts ScanOptions) (ScanIterator, error)
}

type ClientConfig struct {
    Router    Router        // required for smart client mode
    Transport TransportClient
    Pool      NodePool
    Retry     RetryPolicy   // pluggable; default: retry Unavailable, ResourceExhausted
    Auth      Authenticator // optional; attaches credentials to outbound calls
}
```

**Scan semantics**:

- **Single-node**: local `ScanStorage.Scan`
- **Dumb-LB / smart gateway / smart client**: fan out to all nodes, merge streams; supports `ScanOptions{Prefix, Limit, Cursor}`
- **Consistency**: best-effort point-in-time — not a coordinated snapshot unless a future phase adds one

---

## Observability — First-Class Concern

Observability covers four things: metrics, logging, tracing, and state inspection. Metrics tell you something is wrong, logging tells you what happened, tracing shows you the request path, state inspection tells you the current condition of any component.

### Logging

```go
// Every component accepts an optional logger
type NodeConfig struct {
    Logger *slog.Logger // nil = no-op
    Role   NodeRole
    // ...
}
```

Structured fields on every log entry: `node_id`, `shard_id`, `operation`, `key` (hashed, not raw), `duration_ms`, `request_id`, `trace_id`, `error`.

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

### Tracing

Optional interface — users provide their own implementation (OpenTelemetry, etc.):

```go
type Tracer interface {
    StartSpan(ctx context.Context, name string) (context.Context, Span)
}
```

Propagate `request_id` and trace context via gRPC metadata and HTTP headers on every hop (client → gateway/node → forward → replica).

### State Inspection

This is critical for understanding live node, store and cluster state. Every major component exposes a `State()` method returning a structured, serialisable snapshot:

```go
// Node-level state
type NodeState struct {
    ID             string
    Addr           string
    Status         NodeStatus // Active, Receiving, Transferring
    ShardCount     int
    Shards         []ShardState
    PeerCount      int
    Peers          []PeerState
    ReplicationLag time.Duration
    PendingHints   int
}

// Per-shard state
type ShardState struct {
    ID            uint32
    KeyCount      int
    Tokens        float64       // current token bucket level
    Status        ShardStatus   // Active, Transferring, Receiving
    OwnerNode     string
    TransferState TransferState // phase, keys transferred, checkpoint version
}

// Cluster-level state
type ClusterState struct {
    Nodes       []NodeState
    TotalKeys   int
    TotalShards int
    RingHash    uint32 // fingerprint of current ring config
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

### Lifecycle

Graceful shutdown sequence:

1. Stop accepting new requests
2. Drain in-flight requests (configurable timeout)
3. Leave cluster (`Membership.Leave`)
4. Shutdown transport (`TransportServer.Shutdown`)

---

## gRPC Service Definitions

### Client-Facing (configurable port, default convention)

Bound on `ClientFacing` and `Gateway` roles only.

```protobuf
service KVService {
    rpc Get(GetRequest) returns (GetResponse);
    rpc Put(PutRequest) returns (PutResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc Scan(ScanRequest) returns (stream ScanResponse);
}
```

### Internal Cluster (separate configurable port)

Bound on all node roles except pure gateway (gateway may bind for membership RPCs only).

```protobuf
// Request forwarding — when a node receives a request it doesn't own
// Kept separate from KVService to carry forwarding metadata (hop count, origin node)
// x-forwarded metadata prevents infinite forwarding loops
// Not used on smart gateway hot path
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

Node behavior depends on `NodeRole`:

| Role | Servers | Clients |
| ---- | ------- | ------- |
| `ClientFacing` | gRPC on client-facing + internal ports | `NodePool` to all peers |
| `StorageOnly` | gRPC on internal port only | `NodePool` to all peers |
| `Gateway` | gRPC on client-facing port | `NodePool` to all storage nodes |

Connection rules:

- One persistent connection per peer, created at startup, reused for all RPCs
- Never open/close connections per request
- gRPC handles reconnection automatically on transport failure — just retry the RPC
- Keepalive config must match on both client and server sides or connections drop silently
- Retryable error codes: `Unavailable`, `ResourceExhausted` — never retry `NotFound`, `InvalidArgument`

---

## Sharding Architecture

Two independent levels with distinct responsibilities:

```mermaid
flowchart TD
  Key[key] --> Hash1["hash(key) → clusterToken"]
  Hash1 --> Ring["Consistent hash ring → NodeID"]
  Key --> Hash2["hash(key) → shardID mod 256"]
  Hash2 --> LocalShard["Local shard map on node"]
```

**Cluster level** — consistent hash ring maps keys to owning nodes

- Virtual nodes (100-200 per physical node) smooth uneven distribution
- Adding/removing a node only remaps keys in the affected range
- `static shard router` is an alternative cluster-level router (predefined key-range → node map), not the node-level 256-shard map

**Node level** — each node maintains a local shard map (~256 shards)

- Local concurrency partition only — not independently movable across nodes
- Each shard has its own RWMutex — concurrent ops on different key ranges don't block each other
- Each shard has its own token bucket for burst capacity
- Powers of 2 for shard count makes modulo a cheap bitwise op
- Cluster transfer moves keys by token range, not by local shard ID

**Ownership source of truth**: `Membership` + `Router` generation number. Nodes reject writes for stale generations (`ErrTransferInProgress`).

Key lookup path:

```
key → hash → clusterToken → NodeID (via ring)
key → hash → shardID (mod 256, local concurrency partition)
```

---

## Node Join / Shard Transfer Protocol

When a new node D joins and claims a range currently owned by node A:

1. D announces `StateReceiving` to the cluster (membership assigns a generation token)
2. A identifies keys in D's range by scanning its local store
3. A streams those keys to D via `TransferShard` (streaming gRPC)
4. During transfer: **writes go to both A and D**, reads go to A only
5. Cutover criteria met (see below) — D announces `StateReady`
6. Cluster switches: all reads and writes now go to D
7. A deletes its copy of the transferred keys

The double-write window ensures writes during transfer are not lost. Sequence numbers on each shard track ordering through the transfer window.

### Cutover criteria

D is ready when all of the following hold:

1. Bulk `TransferShard` stream completes with a `TransferCheckpoint` (last key + version)
2. D has applied all double-writes since checkpoint (tracked via per-shard sequence number)
3. Merkle root of D's range matches A's (optional verification step)

### Failure recovery

| Failure | Behavior |
| ------- | -------- |
| D crashes mid-transfer | A continues serving; transfer restarts from checkpoint |
| A crashes mid-transfer | D discards partial state; new owner re-transfers |
| Concurrent join on same range | Membership serializes via generation token; second join rejected |
| Write during transfer | Double-write to A + D; sequence numbers ensure ordering |

`TransferState` on `ShardState` exposes phase, keys transferred, and checkpoint version for observability.

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

| Spike duration | Right tool |
| -------------- | ---------- |
| Seconds | Per-shard token bucket |
| Minutes | Adaptive capacity (global pool) |
| Hours+ | Add nodes (planned, off-peak) |

---

## Incremental Build Plan

### Phase 1 — Foundation

- Define core interfaces: `Storage`, `TransportServer`, `TransportClient`
- Define all core error types (including `ErrUnauthorized`, `ErrForbidden`, `ErrInvalidArgument`)
- In-memory map storage implementation
- HTTP transport implementation
- Single node, no distribution, works end to end
- `slog` logging wired into every component from day one
- `StorageState` and `NodeState` structs + `State()` methods on all components
- Noop `Authenticator` / `Authorizer` (auth hooks present, no enforcement)
- `testing/` package: mock Storage, mock TransportClient

### Phase 2 — Persistence (two paths, user chooses)

- **Path A — bbolt**: bbolt storage implementation, bbolt handles durability internally, no WAL needed
- **Path B — hand-rolled WAL**: implement append-only WAL log, recovery on startup, compaction — learning implementation to understand how bbolt works under the hood
- `Scan` added to storage interface (required by both paths)
- `StorageState.SizeBytes` populated
- Optional `TTLStorage` on bbolt path if backend supports it

### Phase 3 — Node-level Sharding

- Shard map within a single node (~256 shards)
- Per-shard RWMutex
- Per-shard token bucket (burst capacity)
- Global node-level token pool (adaptive capacity)
- `Router` interface introduced (passthrough initially)
- `ShardState` fully populated, token bucket level observable via state
- `Metrics` interface introduced, wired into shard operations
- `TTLStorage` on in-memory map (Phase 3 primary TTL path)

### Phase 4 — Static Multi-Node

- gRPC transport implementation
- `NodePool` interface and persistent connections to peers
- Static cluster config
- Consistent hash ring router
- Request forwarding (`ForwardService`) when node doesn't own a key
- Dumb-LB + peer forwarding documented as default multi-node topology
- `KVService` and `ForwardService` protos
- `PeerState` populated in `NodeState`
- Optional `/debug/state` HTTP endpoint on client-facing server
- `NodeRole` config (`ClientFacing`, `StorageOnly`, `Gateway`)

### Phase 4b — Client & LB Config

- `client/` facade (`Client` interface, `ClientConfig`, retry policy)
- `LBConfigSync` watches membership, emits backend list
- Reference `LBConfigSink`: file writer (nginx upstream block), stdout, noop

### Phase 5 — Dynamic Cluster

- Node join/leave protocol
- Shard transfer streaming (`ShardService`)
- Double-write window during transfers
- Transfer checkpoint + failure recovery
- Membership generation tokens (serialize concurrent joins)
- `ClusterService` proto
- `ClusterState` fully observable — all node states, shard ownership, ring fingerprint
- Transfer progress logged and exposed via state inspection (`TransferState`)

### Phase 5b — Smart Gateway

- `gateway/` package composing `Router`, `Membership`, `NodePool`, auth hooks
- Smart gateway topology — routes directly to owner, no `ForwardService` on hot path
- `StorageOnly` node role for backend nodes behind gateway

### Phase 6 — Replication

- `ReplicationService` proto
- `Operation` / `Version` types wired through write path
- Leaderless quorum implementation
- Configurable N, R, W
- Hinted handoff (`HintStore`)
- Read repair
- Idempotency via `RequestID` dedup on replicas
- Replication lag and pending hints observable via `NodeState`

### Phase 7 — Advanced (two paths per item, user chooses)

- **Gossip membership**:
  - Path A: hand-rolled gossip protocol (learning)
  - Path B: use existing gossip library
- **Raft consensus**:
  - Path A: hand-rolled Raft implementation using `RaftService` proto (learning)
  - Path B: `hashicorp/raft` which owns its own transport (production)
- Adaptive capacity at cluster level
- Leader-based replication
- Auth reference implementations: static API key, JWT
- mTLS on internal cluster transport
- `Tracer` interface + request_id/trace_id propagation
- `CASStorage` implementations

---

## gRPC Services by Phase

| Phase | Services | Binding notes |
| ----- | -------- | ------------- |
| Phase 4 | `KVService`, `ForwardService` | `KVService` on ClientFacing + Gateway; `ForwardService` on ClientFacing only |
| Phase 4b | (no new services) | Client uses existing `KVService` |
| Phase 5 | + `ShardService`, `ClusterService` | Internal port on all storage nodes |
| Phase 5b | (no new services) | Gateway binds `KVService` only |
| Phase 6 | + `ReplicationService` | Internal port |
| Phase 7 | + `RaftService` (or delegate to hashicorp/raft) | Internal port |

---

## CLI Client Pattern

For any CLI tooling built on top of the library:

- Use the `client/` facade — not raw `TransportClient`
- One connection per process invocation (not per request)
- Always set `context.WithTimeout` — never hang on a dead server
- Use `os.Exit(1)` on errors for script composability
- Config priority: flag > env var > config file (kubectl/redis-cli pattern)
- No keepalives needed — connection is too short-lived to matter
- If adding a REPL/shell mode: dial once on shell start, reuse for all commands

---

## Open Design Questions

- Extension mechanism for users who need richer APIs than the base interface provides — e.g. a Redis adapter that wants to expose INCR or EXPIRE
- How to expose `ClusterState` to external operators — HTTP endpoint, gRPC reflection, both?
- Backup/export format — future phase, out of scope for v1

---

## Out of Scope (v1)

- Encryption at rest (backend-dependent; see Storage section)
- Backup/restore tooling
- Coordinated snapshot Scan across cluster

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
