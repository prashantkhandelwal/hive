I’ll trace the request path, runtime/concurrency model, and any benchmark configuration to explain the throughput with concrete code references.

Find files matching `*`

Find files matching `**/{README*,package.json,pyproject.toml,go.mod,Cargo.toml,build.gradle*,pom.x...`

Search for `3000|4000|requests?\/s|requests? per second|req\/s|RPS|benchmark|throughput`

Find files matching `src/**/*.rs`

Find files matching `tests/**/*.rs`

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\README.md"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\Cargo.toml"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\kiryuu.service"
}

Find files matching `*.{toml,json,yml,yaml}`

The throughput claim is production-observed rather than a load-test result. I’m now following `/announce` through parsing, shared-state access, response generation, and deployment tuning to separate architectural causes from operational settings.

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\main.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\lib.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\handlers\\mod.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\store\\mod.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\query\\mod.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\byte_functions\\mod.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\byte_functions\\types.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\req_log\\mod.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\store\\stats.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\blacklist.rs"
}

Read , lines 1 to 220
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\main.rs",
  "view_range": [
    1,
    220
  ]
}

Read , line 221 to the end
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\main.rs",
  "view_range": [
    221,
    -1
  ]
}

Search for `^(pub )?(struct|enum|type|fn|impl)|^\s+pub fn|^\s+fn `

Search for `DashMap|RwLock|Mutex|Atomic|thread|spawn|shard|stripe|cache|announce|peer|gc|...`

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\handlers\\metrics.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\.github\\workflows\\build.yml"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\.github\\workflows\\bench.yml"
}

Read , lines 1 to 240
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\store\\mod.rs",
  "view_range": [
    1,
    240
  ]
}

Read , lines 241 to 474
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\store\\mod.rs",
  "view_range": [
    241,
    474
  ]
}

Read , lines 475 to 665
Completed with input: {
  "path": "F:\\Github\\kiryuu\\src\\store\\mod.rs",
  "view_range": [
    475,
    665
  ]
}

Search for `kiryuu_http_request_duration|rate\(|histogram_quantile|active_request|residen...`

Ran terminal command: git --no-pager log --oneline -15

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\benches\\url_enc_to_hex.rs"
}

Read 
Completed with input: {
  "path": "F:\\Github\\kiryuu\\benches\\ip_str_to_bytes.rs"
}

Ran terminal command: git --no-pager show --stat --oneline a6271b2; git --no-pager show --stat --oneline d233bce; git --no-pager log --oneline --all --grep="performance\|perf\|redis\|cache\|memory\|lock" -i -25

Search for `cache_hit|record_cache_hit`

Search for `workers\(|worker_max_blocking_threads|keep_alive|max_connections|backlog|clie...`

The key mechanism is now clear: this is a bounded, entirely in-memory hot path with sharded locking, while full-map maintenance is deliberately moved off Actix workers. I’m checking the production metrics definitions and practical bottlenecks before summarizing what the 3–4k figure does—and does not—prove.

Read , lines 275 to 315
Completed with input: {
  "path": "F:\\Github\\kiryuu\\grafana-dashboard.json",
  "view_range": [
    275,
    315
  ]
}

Read , lines 620 to 690
Completed with input: {
  "path": "F:\\Github\\kiryuu\\grafana-dashboard.json",
  "view_range": [
    620,
    690
  ]
}

Read , lines 700 to 735
Completed with input: {
  "path": "F:\\Github\\kiryuu\\grafana-dashboard.json",
  "view_range": [
    700,
    735
  ]
}

Ran terminal command: git --no-pager show --format=fuller --no-ext-diff --unified=25 ed87fa1 -- src/store/mod.rs Cargo.toml

Ran terminal command: git --no-pager status --short

## Short answer

Kiryuu reaches roughly **3,000–4,000 announces/second because each request performs a small, bounded amount of work entirely in memory**. There is no database, Redis call, disk access, or outbound network request in the `/announce` path.

The claim is also internally consistent:

- 290 million requests/day ÷ 86,400 seconds ≈ **3,356 requests/second average**.
- The repository’s Grafana dashboard says that at this load, nearly all handler executions fall within its bottom five latency buckets—up to **1 ms** (`grafana-dashboard.json`).
- On two CPU cores, 4,000 requests/second provides about **500 μs of CPU time per request** if both cores are saturated. This implementation’s normal case plausibly fits within that budget.

## Why the request path is fast

### 1. All tracker state is held in process memory

The central store is a `DashMap` shared through an `Arc` (`TrackerStore`). The request handler directly calls `handle_announce`; it does not wait on external services.

That eliminates the latency and connection-pool overhead normally associated with:

- SQL queries
- Redis round trips
- Disk writes
- IPC
- Network timeouts

This is probably the single largest reason for the throughput.

The tradeoff is that all swarm state disappears when the process restarts, as documented in the `README`.

### 2. State is sharded rather than protected by one global store lock

`DashMap` divides keys among internal shards. Requests concerning info hashes in different shards can execute concurrently instead of contending on a single `Mutex`.

It also uses AHash’s `RandomState`, a fast non-cryptographic hash implementation appropriate for an in-memory map.

Each torrent is modified through `get_mut`, so requests that land in the same shard—and particularly the same torrent—still serialize temporarily. The design benefits significantly when traffic is distributed among many info hashes.

### 3. Peer records are extremely compact

The tracker does not retain rich peer objects or strings:

- An info hash is `[u8; 20]`.
- A peer is six bytes: four bytes of IPv4 address plus two bytes of port (`type definitions`).
- Seeder status and expiration are packed into one `u32` (`PeerMeta`).

Small records improve cache locality and substantially reduce allocation and memory-bandwidth costs.

For the common case of eight peers or fewer, peers are stored directly in a fixed inline array instead of allocating a separate hash table (`Peers`). Only larger swarms are promoted to a `HashMap`.

### 4. Response size is capped

Each response samples at most:

- 50 seeders
- 50 leechers

This is controlled by `PEER_SAMPLE_LIMIT`. Compact BitTorrent peers consume six bytes each, so peer data is capped at approximately **600 bytes**, plus a small bencoded envelope.

Therefore, a large swarm does not produce a proportionally large network response.

### 5. Parsing uses fixed-size byte representations

The query is deserialized and the info hash is converted directly into a stack-allocated `[u8; 20]` (`parse_announce`, `url_encoded_to_raw_u8`).

IPv4 and port conversion likewise produces a fixed `[u8; 6]` (`ip_str_port_u16_to_bytes`).

There are still allocations—the query string replacement, `serde_qs`, peer vectors, and response construction—but the resulting data structures are small and bounded.

### 6. Garbage collection is removed from request workers

Expiration cleanup could be expensive because it must inspect tracker state. Kiryuu explicitly runs it on a separate OS thread (`main.rs`).

Furthermore, each cleanup pass scans only one of 256 stripes rather than the complete torrent map (`sweep_stale_torrents_at`). This spreads cleanup work over time and prevents periodic full-map pauses from blocking Actix workers.

Expensive global peer totals are also recomputed on that background thread every 60 seconds. The metrics endpoint reads cached atomics rather than traversing the store during every Prometheus scrape (`refresh_peer_totals`).

### 7. Request metrics are cheap and lock-free

Most counters use relaxed atomic operations (`record_announce`). Relaxed ordering is sufficient for observational statistics and avoids unnecessary memory synchronization.

There are multiple atomic increments for the latency histogram, but the work is bounded to 17 buckets.

The code also deliberately avoids logging malformed hashes on every request. It increments an atomic counter instead (`byte_functions/mod.rs`). This prevents an attacker or malformed client from turning stdout or journald locking and I/O into a throughput bottleneck.

Tracing is optional and is not compiled into the normal build.

### 8. Rust, Actix, release compilation, and jemalloc

The HTTP server uses Actix Web (`Cargo.toml`), which provides an efficient multithreaded socket server. No explicit worker count is configured, so Actix uses its default based on available CPU parallelism.

The handler itself contains no meaningful asynchronous I/O—it is an `async fn`, but the important work is synchronous and in-memory. Thus, **async is not the main reason for the speed**. The important factors are short CPU work, multiple Actix workers, and sharded state.

The binary also:

- Uses jemalloc globally (`main.rs`).
- Is intended to be compiled in release mode.
- Recommends `-C target-cpu=native` for CPU-specific optimization (`README.md`).

## Operational tuning that keeps the server accepting traffic

The server exposes tuning for:

- TCP accept backlog
- Maximum concurrent connections
- Open-file limit
- One-second client request timeout

See the server setup in `main.rs` and the production unit in `kiryuu.service`.

These settings do not make the request computation faster, but they prevent the application from failing under connection bursts or exhausting file descriptors.

The service also discards stdout because normal request-path logging is intentionally absent (`kiryuu.service`).

## What the number does not prove

There is no end-to-end HTTP load test in this repository. The Criterion benchmarks only cover two primitive conversion functions:

- `url_enc_to_hex.rs`
- `ip_str_to_bytes.rs`

The 3–4k figure therefore appears to be a **production observation**, corroborated through the Prometheus request counter:

```promql
rate(kiryuu_http_request_count[$__rate_interval])
```

That dashboard expression is defined in `grafana-dashboard.json`.

There are two measurement qualifications:

1. The request counter is incremented after a normal announce reaches the store. Rejected requests and blacklist responses are not included.
2. Handler duration ends after state processing and response-body creation. It does not include the complete client-side network round trip or necessarily all response transmission time.

## Likely scaling limits

The current design should handle this observed workload well, but it is not unlimited:

1. **Hot torrent contention:** `handle_announce` holds a mutable DashMap guard while purging, counting, sampling, constructing the response, and updating the peer. Many requests for one torrent can serialize.

2. **Peer scans:** Every announce counts all peers and samples seeders and leechers. The 50-peer response is bounded, but the implementation still traverses the collection multiple times. Very large individual swarms will cost more.

3. **Global active-request mutex:** Every main-server request increments and decrements one `parking_lot::Mutex` counter in `main.rs`. It is held very briefly, but at much higher rates it could become cache-line contention.

4. **Allocation-heavy response construction:** `announce_reply` performs several string and vector concatenations. These are small, and jemalloc helps, but a directly preallocated byte buffer would be more efficient.

5. **IPv4 only:** Compact six-byte peers and the current parser reject IPv6, which simplifies and reduces the request path.

In summary, **3–4k requests/second is achieved through a deliberately narrow tracker workload: compact representations, bounded responses, in-memory state, sharded locking, no request logging, and background maintenance**. It is not primarily an “async” achievement, and it does not establish the maximum throughput without an end-to-end saturation benchmark.