# KV Store — LLM Agent Integration Patterns

This document covers how the library is used as infrastructure for LLM agent systems. It does not replace [DESIGN.md](DESIGN.md) — read that first for architecture and interface definitions. This document focuses on access patterns, adapter sketches, and design decisions that were directly informed by agent workloads.

---

## What This Library Is (and Is Not) for Agents

This library is **not** a vector database. It does not understand embeddings, similarity search, or semantic retrieval. Use a purpose-built vector store (Pinecone, Weaviate, pgvector, Chroma, etc.) for those workloads.

This library is **infrastructure for agent state** — the durable, keyed data that agents read and write as they operate: sessions, checkpoints, tool outputs, config, short-lived working memory with TTL, and coordination primitives between concurrent agent processes.

---

## Semantic Search vs KV Split

LLM agent systems typically need two storage layers with different access patterns:

| Layer | Purpose | Access pattern | Store |
| ----- | ------- | -------------- | ----- |
| **Vector / semantic** | Find relevant context by meaning | ANN similarity search | Dedicated vector DB |
| **Structured KV** | Agent state by known key | Exact lookup, TTL, CAS | This library |

A common mistake is trying to unify these into one store. The access patterns are incompatible: similarity search requires inverted indexes over high-dimensional floats; KV lookup requires O(1) hash-ring routing. Keep them separate and let each do what it is good at.

**What lives in this library, not in the vector store:**
- Agent session state (conversation turns, in-progress tool calls)
- Checkpoint blobs for resume/fork (LangGraph-style)
- Deduplicated tool result cache (key = hash of tool + args)
- API keys and per-agent quota config
- Lock tokens for distributed agent coordination
- Short-lived scratchpad keys with TTL

**What lives in the vector store:**
- Document embeddings
- Semantic search indexes
- Long-term memory retrieved by similarity

---

## Agent Access Patterns

### 1. Short-lived session keys with TTL

Agents create a session at conversation start and need it to expire automatically if the conversation is abandoned. The library's `Entry.ExpiresAt` field handles this at the storage layer — no separate eviction job required.

```go
// Via TTLStorage convenience API — converts duration to Entry.ExpiresAt
err := store.PutWithTTL(ctx, "session:"+sessionID, sessionBytes, 30*time.Minute)

// On read, expired entries return ErrKeyNotFound automatically
data, err := store.Get(ctx, "session:"+sessionID)
if errors.Is(err, kvstore.ErrKeyNotFound) {
    // session expired — start fresh
}
```

**Access pattern:** write once on session start, read on each turn, TTL ~15–60 minutes. Low value size (< 10 KB per session). High read/write rate in active systems.

**Topology recommendation:** smart client (lowest latency, no forwarding hop) or smart gateway (centralized auth for multi-tenant systems).

### 2. Checkpoint / resume blobs (LangGraph-style)

Long-running agent graphs checkpoint their state so they can resume after a crash, be forked for parallel exploration, or be inspected by a human.

```go
// Write checkpoint at graph node boundary
checkpointKey := fmt.Sprintf("checkpoint:%s:%s", runID, nodeID)
err := store.Put(ctx, checkpointKey, checkpointBytes)

// Resume: read latest checkpoint and replay from that node
data, err := store.Get(ctx, checkpointKey)
```

**Access pattern:** write on each graph node completion, read only on resume. Value size varies widely (10 KB – 1 MB depending on context window included). Write throughput is moderate; durability matters more than latency.

**Storage recommendation:** bbolt-backed `DataStore` for durability. Replication (Phase 6) for multi-node HA.

### 3. Idempotent tool result caching

Tools that call external APIs (web search, code execution, database queries) are expensive and often deterministic given the same inputs. Cache their results to avoid redundant calls within a run or across retries.

```go
cacheKey := fmt.Sprintf("tool:%s:%x", toolName, sha256.Sum256(argsBytes))

// Check cache first
cached, err := store.Get(ctx, cacheKey)
if err == nil {
    return cached // cache hit
}

// Execute tool, store result with TTL
result, err := executeTool(ctx, toolName, argsBytes)
if err == nil {
    _ = store.PutWithTTL(ctx, cacheKey, result, 1*time.Hour)
}
```

**Access pattern:** read-heavy with occasional writes. TTL matches the freshness requirement of the data source. CAS (`CASStorage`) is not needed here — last-write-wins on cache population is fine.

**Post-MVP note:** `DedupStore` (Phase 6) adds exactly-once semantics for tool execution across replicated nodes, useful when multiple agent replicas might race to populate the same cache entry.

### 4. Distributed lock tokens for agent coordination

When multiple agent processes collaborate (e.g. a research agent spawning sub-agents), they need coordination primitives. A simple distributed lock using CAS:

```go
// Acquire lock: CAS from empty → token
acquired, err := store.(kvstore.CASStorage).CompareAndSwap(
    ctx,
    "lock:"+resourceID,
    nil,          // expected: key does not exist
    lockBytes,    // new: lock token with owner + expiry
)
if !acquired {
    // another agent holds the lock
}

// Release: CAS from our token → empty
released, err := store.(kvstore.CASStorage).CompareAndSwap(
    ctx,
    "lock:"+resourceID,
    lockBytes,
    nil,
)
```

This is a single-node CAS lock — sufficient when all agents talk to the same node (passthrough router) or the key is consistently routed to the same node via the hash ring. Cross-node distributed locks are out of scope v1.

### 5. Fan-out Scan for admin / observability

Not an agent hot-path operation — used by admin tooling, monitoring, and bulk cleanup jobs. In a multi-node deployment, `Client.Scan` fans out to all nodes and merges streams.

```go
iter, err := client.Scan(ctx, kvstore.ScanOptions{Prefix: "session:", Limit: 1000})
for iter.Next() {
    k, v := iter.Key(), iter.Value()
    // inspect or clean up expired sessions
}
```

**Important:** fan-out Scan is not a coordinated snapshot. Concurrent writes during a scan may or may not be visible. Do not use it for financial reconciliation or operations that require a consistent view. See Open Design Questions in DESIGN.md for planned alternatives.

---

## Adapter Sketches

These adapters are not implemented in the library. They are thin wrappers that agent framework authors can build on top of the library's interfaces. No adapter code ships in MVP — the interfaces are documented here so users can wire them up.

### LangGraph / LangChain checkpointer

LangGraph's `BaseCheckpointSaver` interface requires `get`, `put`, and `list` operations keyed by run ID and checkpoint ID. This maps directly onto the library's `Storage`:

```go
type LangGraphCheckpointer struct {
    store kvstore.Storage
}

func (c *LangGraphCheckpointer) Put(ctx context.Context, config RunnableConfig, checkpoint Checkpoint) error {
    key := fmt.Sprintf("lg:checkpoint:%s:%s", config.RunID, checkpoint.ID)
    data, _ := json.Marshal(checkpoint)
    return c.store.Put(ctx, key, data)
}

func (c *LangGraphCheckpointer) Get(ctx context.Context, config RunnableConfig) (*Checkpoint, error) {
    key := fmt.Sprintf("lg:checkpoint:%s:latest", config.RunID)
    data, err := c.store.Get(ctx, key)
    if errors.Is(err, kvstore.ErrKeyNotFound) {
        return nil, nil
    }
    // ...unmarshal and return
}
```

The library's hash ring routes all keys for a given `runID` to the same node (because `runID` is the key prefix and `hash(key)` is consistent). This means checkpoint reads and writes for a run are always co-located — no cross-node coordination needed.

### Generic `Store` interface for agent frameworks

Many agent frameworks define a minimal `Store` interface (get/set/delete by string key). The library's `Storage` interface is intentionally compatible:

```go
// agent framework's interface — already satisfied by kvstore.Storage
type AgentStore interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Set(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
}

// direct assignment — no wrapper needed if the framework uses []byte values
var agentStore AgentStore = myKVNode // implements Storage
```

If the framework uses typed values (string, JSON), add a thin serialization wrapper. The library intentionally stays at `[]byte` to avoid imposing a serialization format on users.

---

## Design Decisions Informed by Agent Workloads

Several library design choices were directly shaped by the agent use case:

**TTL-first via `Entry.ExpiresAt`:** Agent session state has a natural lifecycle. TTL baked into the storage layer (rather than requiring users to build eviction) reduces the surface area for memory leaks in long-running agent systems.

**`[]byte` values, no schema:** Agent frameworks use many different serialization formats (JSON, msgpack, protobuf, pickle). The library deliberately does not impose one. Users serialize at the application layer; the library stores opaque bytes.

**Auth at the gateway, not per-node:** In a multi-tenant agent system (many users, many agents), centralized auth at a smart gateway is cleaner than per-node auth. The smart gateway topology routes all traffic through a gateway that holds `CredentialStore` — storage nodes run `StorageOnly` and never see raw credentials.

**Smart client for latency-sensitive tool loops:** Agent tool calls that hit the KV store on every step (e.g. reading session state before each LLM call) benefit from the smart client topology — the client SDK embeds the `Router` and talks directly to the owning node, eliminating the forwarding hop. This is important when agent step latency is dominated by network round trips.

**Hash ring key affinity:** All keys for a given agent run (session, checkpoints, tool cache) share a common prefix (`run:<runID>:`). Because the consistent hash ring routes by key, keys with the same prefix hash to the same node. This means all state for a run is co-located — reads and writes for a run never require cross-node coordination.

---

## Cross-References to DESIGN.md

| Topic | DESIGN.md section |
| ----- | ----------------- |
| `Entry.ExpiresAt` and TTL | Storage (data plane) → Internal storage record |
| `TTLStorage` interface | Storage (data plane) |
| `CASStorage` interface | Storage (data plane) |
| Smart client topology | Deployment Topologies → Smart client |
| Smart gateway topology | Deployment Topologies → Smart gateway |
| Auth + CredentialStore | Security |
| Hash ring routing | Sharding Architecture |
| Scan fan-out semantics | Client API → Scan semantics |
| Phase delivery timeline | Incremental Build Plan |
