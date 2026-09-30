# Peer Expiry Improvement Roadmap

## Context

Periodic maintenance removes peers whose `last_seen` timestamp is older than
the configured peer timeout. Cleanup currently runs in `spawn_blocking`, which
protects Tokio worker threads from synchronous work, but it does not eliminate:

- work proportional to the total number of tracked peers;
- DashMap shard lock contention while swarms are retained;
- latency spikes when many peers expire together; or
- the separate traversal of completion records.

The default configuration currently uses:

- a 1,800-second announce interval;
- a 3,600-second peer timeout; and
- a 30-second persistence and maintenance interval.

## Implemented Low-Risk Optimization

Stale seeders and leechers are counted while the existing peer `retain`
operation decides which peers to remove. Swarm counters are then decremented
from those values.

This avoids rescanning all surviving peers in a changed swarm and does not add
locks, allocations, or bookkeeping to the announce path.

Periodic maintenance also records `cleanup_duration_ms` in its debug log. This
provides an initial signal for deciding whether further optimization is
necessary.

## Recommended Next Step: Measure First

Do not make another structural cleanup change until the current behavior has
been measured under a workload that actually runs stale-peer cleanup.

The existing application load benchmark configures a 3,600-second persistence
interval, so cleanup is unlikely to run during a normal benchmark execution.
It measures announce throughput, but it does not measure contention caused by
maintenance.

### Cleanup Benchmark

Add an ignored, release-mode benchmark that:

1. Preloads a configurable number of swarms and peers.
2. Includes a realistic distribution of small and large swarms.
3. Marks a configurable percentage of peers as stale.
4. Runs concurrent announces while invoking stale cleanup.
5. Captures cleanup and request performance separately.

At minimum, record:

- cleanup duration;
- announce throughput;
- p50, p95, p99, and maximum announce latency;
- CPU consumption;
- peak memory;
- swarms and peers examined; and
- peers removed.

Suggested scenarios:

| Peers | Stale peers |
|------:|------------:|
| 10,000 | 0% |
| 10,000 | 10% |
| 100,000 | 0% |
| 100,000 | 10% |
| 1,000,000 | 1% |

The zero-percent scenarios are important because full scanning still occurs
when there is nothing to remove.

### Production Observation

Use `cleanup_duration_ms` to establish:

- typical and maximum cleanup duration;
- growth relative to peer population;
- whether announce latency rises during cleanup; and
- whether cleanup approaches or exceeds the maintenance interval.

Add a Prometheus histogram only if debug logs are insufficient for this
analysis.

## Conditional Improvements

Apply the following changes only when measurements identify the corresponding
bottleneck.

### 1. Target Completion Cleanup

The current cleanup performs a second full pass over completion records. If
this pass is significant:

1. Perform one full orphan-completion cleanup after persistence is loaded.
2. During periodic cleanup, inspect completion records only for swarms that
   became empty.
3. Preserve records whose completed-download count is non-zero.
4. Add concurrency tests for an announce arriving while an empty swarm and its
   completion record are being removed.

This requires care because swarms and completion records are stored in
separate DashMaps. The change must define and test consistent lock ordering and
concurrent mutation behavior.

### 2. Shorten DashMap Lock Hold Time

If announce tail latency rises during cleanup, replace whole-map `retain` with
per-swarm processing:

1. Snapshot the swarm keys.
2. Process each swarm through its individual entry.
3. Release the DashMap shard lock between swarms.
4. Benchmark the extra key allocation and lock acquisitions against the
   improvement in p99 announce latency.

This does not reduce total scanning work, but it can reduce the maximum time an
announce waits for a shard lock.

### 3. Bound Cleanup Work

If per-swarm processing remains disruptive, process cleanup under a swarm-count
or wall-clock budget, for example:

```text
process at most 512 swarms or 10 milliseconds
yield
continue with the next batch
```

The implementation must:

- eventually visit every swarm;
- prevent starvation when the swarm set changes;
- expose cleanup backlog or cycle duration; and
- define the acceptable delay beyond `peer_timeout`.

This approach spreads cleanup work over time without adding work to normal
announces.

### 4. Introduce an Expiry Index

Use an expiry wheel or timestamp buckets only if benchmarks show that full
scans remain the dominant cost after lower-risk improvements.

An expiry entry would contain:

```text
(info_hash, peer_id, expected_last_seen)
```

When an entry expires, remove the peer only when its current `last_seen` still
matches `expected_last_seen`. A reannounce can schedule a new record without
removing the old one.

Before adopting this design, validate:

- announce-path lock contention;
- memory retained by obsolete expiry entries;
- frequent or malicious reannounce patterns;
- restored peers;
- stopped and blacklisted peers;
- bounded processing when many entries expire together; and
- queue size and obsolete-entry metrics.

With the default announce and timeout values, a normally behaving active peer
would have approximately two outstanding expiry records. This may be an
acceptable trade-off at large scale, but it must be confirmed by an A/B
benchmark.

## Decision Guide

| Measurement | Preferred response |
|---|---|
| Cleanup duration and announce latency are acceptable | Keep the current implementation |
| Completion traversal dominates cleanup | Target completion cleanup |
| DashMap lock waits dominate p99 latency | Process swarms individually |
| Cleanup work is acceptable but produces spikes | Add bounded batching |
| Full scanning remains CPU-heavy at target scale | Evaluate an expiry index |

## Validation Requirements

Every future cleanup change should preserve and test:

- peer, seeder, leecher, swarm, and torrent counters;
- completed-download history after the last peer expires;
- removal of zero-download torrents with no peers;
- restored stale peers at startup;
- reannounces concurrent with cleanup;
- stopped announces;
- blacklist removal;
- dirty-state persistence notifications; and
- HTTP and UDP announce latency during maintenance.
