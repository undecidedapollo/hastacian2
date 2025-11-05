# Hastacian Development Plan

## Vision

Building fundamental distributed primitives that are as easy to use as `HashMap<String, XX>`, but distributed across a cluster with Raft providing consensus and persistence behind the scenes.

**Core Idea:** Write code that works on distributed data, with the code itself owning it via Raft as the persistence store. Clean, batteries-included primitives that you can use, with libraries/frameworks built on top. No separate clusters (Redis, Zookeeper, etcd, Consul) to run.

**Target UX:**
```rust
use hastacian::DHashMap;

#[tokio::main]
async fn main() {
    let map = DHashMap::new("cluster-config").await?;
    map.insert("key", "value").await?;  // Replicated automatically
}
```

Just `cargo run` 3 times across 3 machines and it works.

## Mental Model Check

### What We're Building
✅ **Raft as invisible backend** - Users never see it
✅ **Batteries-included primitives** - Lock, KV, Queue, PubSub
✅ **In-process, not sidecar** - Just a library dependency
✅ **No separate cluster** - The app instances ARE the cluster
✅ **Actor model on top** - Natural next layer

### Key Design Questions to Address

**1. Node Discovery**
- How do the 3 instances find each other?
- Hardcoded addresses? DNS? Gossip?
- Dynamic membership (nodes join/leave)?

**2. Sharding Strategy**
- Single Raft group = all data through one log (simple, bottleneck)
- Multiple Raft groups = shard by key (complex, scales)
- Cloudflare does single-instance per object

**3. Error Handling**
- Network failures are real - can't fully hide them
- `map.insert().await?` - what errors can happen?
- Users need to handle `LeaderUnavailable`, `NetworkPartition`, etc.

**4. Consistency Guarantees**
- Strong consistency (always) or tunable?
- Read-your-writes guarantee?
- Linearizable reads (expensive) vs stale reads (fast)?

## Development Phases

### Phase 1: Foundation (Now → 2 weeks)

**Goal:** Get basic membership + DHashMap working beautifully

```rust
// Step 1: Node discovery/membership
// Start simple - hardcoded config file
let cluster = Cluster::new(ClusterConfig {
    node_id: 1,
    nodes: vec![
        (1, "localhost:5001"),
        (2, "localhost:5002"),
        (3, "localhost:5003"),
    ],
}).await?;

// Step 2: DHashMap API design
let map = cluster.dhashmap::<String, String>("my-map").await?;
map.insert("key", "value").await?;
let val = map.get("key").await?;
```

**Why this first:**
- Builds directly on existing peernet + KV
- Forces us to solve discovery
- Tests the ergonomics early

### Phase 2: Core Primitives (2-4 weeks)

#### DLock - Distributed mutex
```rust
let lock = cluster.dlock("my-resource").await?;
let guard = lock.lock().await?;
// Critical section
drop(guard); // Releases lock
```

**Implementation:** Raft log entry for lock acquisition, track owner in state machine

#### DQueue - Durable FIFO queue (Kafka-lite)
```rust
let queue = cluster.dqueue::<Event>("events").await?;
queue.push(event).await?;
let item = queue.pop().await?;
```

**Implementation:** Queue state in Raft, log index as offset

### Phase 3: Messaging (4-6 weeks)

#### DPubSub - Publish/subscribe
```rust
let pubsub = cluster.dpubsub::<Message>("topic").await?;
let mut sub = pubsub.subscribe().await?;

pubsub.publish(msg).await?;
let received = sub.recv().await?;
```

**Implementation:** Track subscribers in state machine, deliver via Raft log

### Phase 4: Actor Runtime (6-8 weeks)

```rust
#[actor]
struct Counter {
    count: i32,
}

impl Counter {
    async fn increment(&mut self) -> i32 {
        self.count += 1;
        self.count
    }
}

// Usage
let actor = cluster.spawn_actor::<Counter>("counter-1").await?;
let result = actor.call(|c| c.increment()).await?;
```

## Architecture

### Start With Single-Group Design

```
Your App (3 instances)
├── User Code
│   └── Uses DHashMap, DLock, etc.
├── Hastacian Library
│   ├── Cluster (handles membership)
│   ├── Primitives (DHashMap, DLock, etc.)
│   └── Raft (single group, all operations)
└── Network Layer (peernet)
```

**Pros:**
- Simple to reason about
- Strongly consistent
- Easy debugging

**Cons:**
- Single bottleneck (leader)
- Doesn't scale past ~10k ops/sec

**Later:** Shard into multiple Raft groups (like CockroachDB/TiKV)

### API Design Philosophy

**Be honest about failures:**
```rust
pub enum DHashMapError {
    NoLeader,
    NetworkTimeout,
    NodeDown,
    SerializationError,
}

// Users must handle distributed reality
map.insert("key", "value").await?;
```

**But hide complexity:**
- Auto-retry on leader changes
- Connection pooling hidden
- Raft details completely abstracted

### Discovery Strategy

**Phase 1: Static config**
```toml
# cluster.toml
[[nodes]]
id = 1
addr = "10.0.0.1:5001"

[[nodes]]
id = 2
addr = "10.0.0.2:5001"
```

**Phase 2: DNS-based**
```rust
// Discovers via DNS SRV records
let cluster = Cluster::discover("my-app.local").await?;
```

**Phase 3: Gossip protocol** (like Consul)

## Immediate Next Steps (This Week)

1. **Refactor current code** into `Cluster` abstraction
2. **Create `DHashMap`** wrapper around existing KV store
3. **Write example app** - something real (URL shortener?)
4. **Test ergonomics** - does it feel magical?

### Target Project Structure
```
hastacian2/
├── src/
│   ├── cluster/         # Membership, discovery
│   ├── primitives/      # DHashMap, DLock, etc.
│   │   ├── dhashmap.rs
│   │   ├── dlock.rs
│   │   ├── dqueue.rs
│   │   └── mod.rs
│   ├── raft/            # Existing Raft impl (refactored)
│   │   ├── store.rs
│   │   ├── log_store.rs
│   │   └── mod.rs
│   └── network/         # peernet
├── examples/
│   ├── url_shortener.rs # Real app using DHashMap
│   ├── counter.rs       # Simple demo
│   └── cluster.toml     # Config for examples
└── README.md            # "Just cargo run 3x"
```

### Example: URL Shortener
```rust
// examples/url_shortener.rs
use hastacian::Cluster;

#[tokio::main]
async fn main() -> Result<()> {
    let cluster = Cluster::from_config("cluster.toml").await?;
    let urls = cluster.dhashmap::<String, String>("urls").await?;

    // Start HTTP server
    let app = Router::new()
        .route("/shorten", post(|long_url: String| async move {
            let short = generate_id();
            urls.insert(short.clone(), long_url).await?;
            Ok(short)
        }))
        .route("/:short", get(|short: String| async move {
            urls.get(&short).await
        }));

    axum::Server::bind("0.0.0.0:3000")
        .serve(app.into_make_service())
        .await?;
}
```

## Similar Systems (For Reference)

- **Cloudflare Durable Objects** - Closest to our vision, but cloud-only
- **Orleans** (.NET) - Virtual actors, but heavy cluster setup
- **Akka Cluster** - Actor model, complex cluster management
- **Dapr** - Sidecar model (not in-process)
- **Restate** - Workflow orchestration, still separate deployment

**What Doesn't Exist:** No Rust library that does in-process distributed primitives with zero cluster setup. This is our opportunity.

## Design Principles

1. **Ergonomics First** - If it doesn't feel magical, we failed
2. **Progressive Disclosure** - Simple by default, powerful when needed
3. **Honest Errors** - Don't hide distributed reality, but make it manageable
4. **Zero Config** - Should work with minimal setup
5. **Production Ready** - Not a toy, build for real use

## Success Criteria

**Phase 1 Success:**
- Can run URL shortener example across 3 nodes
- Code is <50 lines
- "Just works" with minimal config
- Survives node failures gracefully

If we nail this, everything else follows naturally.

## Open Questions

1. How do we handle schema evolution? (versioning serialized data)
2. What's the story for observability? (metrics, tracing)
3. How do we test this? (chaos testing, network partitions)
4. Should we support dynamic membership from day 1?
5. What's the migration path from single-Raft to sharded?

## Future Vision

Beyond Phase 4, we could build:
- **Distributed transactions** (DynamoDB-style)
- **MVCC storage** for time-travel queries
- **Streaming queries** (reactive updates)
- **Workflow engine** (Temporal-like)
- **Service mesh** capabilities

All built on the same Raft foundation with consistent primitives.

---

*"Make distributed systems as boring as HashMap."*
