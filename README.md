## Overview

Hive is a compact BitTorrent tracker written in Rust. A shared DashMap-backed
swarm registry serves HTTP and BEP 15 UDP clients, while SQLite snapshots retain
peer and completion data across restarts.

The service exposes:

* `GET /announce` for compact HTTP announces
* `GET /scrape` for all torrents or one or more repeated `info_hash` parameters
* `GET /health` for database-aware health checks
* `GET /stats?period=day|week|month` for current and historical JSON statistics
* `GET /metrics` for Prometheus text exposition
* `GET /` for the operational dashboard
* UDP connect, announce, and scrape actions on port `6969` by default

Both listeners accept IPv6 addresses. Binding to `[::]` also accepts IPv4 on
platforms where dual-stack sockets are enabled.

## Run Locally

Install the Rust toolchain and the platform linker, then run:

```powershell
cargo run --release
```

Select the tracker protocol with `--protocol`, or use `both` explicitly:

```powershell
cargo run --release -- --protocol http
cargo run --release -- --protocol udp
cargo run --release -- --protocol both
```

Without `--protocol`, Hive uses `default_protocol` from `hive.toml`. Its default
value is `both`, so the HTTP and UDP listeners run when neither option is
configured.

Open `http://localhost:3000` for the dashboard. The default SQLite database is
created as `hive.db` in the working directory. The dashboard remains available
when `udp` is selected; in that mode, the read-only HTTP scrape route remains
available while HTTP announces are disabled.

The single-page dashboard shows peers, seeders, leechers, torrents, completed
downloads, and uptime. Its shared trend chart supports day, week, and month
views. Metric snapshots and daily ingress and egress totals are stored in
SQLite, so transfer totals can be summed across the selected period.
The trend chart uses Apache ECharts 6.1.0 loaded from jsDelivr with a pinned
version and subresource integrity hash, so chart rendering requires access to
the CDN.

Tracker population totals are maintained incrementally for constant-time
telemetry updates. Dashboard history is cached in memory until a new snapshot
is committed, tracker persistence writes only changed torrents, and full HTTP
scrape responses are cached for up to five seconds with mutation-based
invalidation.

## Configuration

Hive reads configuration from `hive.toml` in the working directory.

| Setting | Default | Purpose |
| --- | --- | --- |
| `default_protocol` | `both` | Listener mode: `http`, `udp`, or `both` |
| `http_addr` | `0.0.0.0:3000` | Web UI and HTTP tracker listen address |
| `udp_addr` | `0.0.0.0:6969` | UDP tracker listen address |
| `database_path` | `hive.db` | SQLite database path |
| `auth_token` | Unset | Optional HTTP bearer token |
| `announce_interval` | `1800` | Client reannounce interval in seconds |
| `peer_timeout` | `3600` | Maximum idle peer age in seconds |
| `persistence_interval` | `30` | Snapshot interval in seconds |
| `rate_limit_per_minute` | `120` | Per-source-IP request allowance |
| `log_filter` | `hive_tracker=debug` | Tracing filter and verbosity |

Use `--config path/to/config.toml` to load another file. The `--protocol`
command-line argument overrides `default_protocol` for the current process.

Set `log_filter` to a tracing directive such as `hive_tracker=trace` for maximum
detail or `hive_tracker=info` for quieter operational logs. Multiple directives
can be comma-separated.

When `auth_token` is set, `/announce`, `/scrape`, and `/metrics` require
the following header:

```http
Authorization: Bearer your-token
```

The dashboard, aggregate statistics, and health endpoint remain public. The UDP
tracker uses short-lived source-bound connection IDs and rate limiting, but BEP
15 does not define bearer authentication.

## Client URLs

Use these tracker URLs with their corresponding protocols:

```text
http://tracker.example.com:3000/announce
udp://tracker.example.com:6969/announce
```

HTTP requests must percent-encode the raw 20-byte `info_hash` and `peer_id`.
Announce requests follow BEP 3 and must include `port`, `uploaded`,
`downloaded`, and `left`. Responses use BEP 23 compact peer encoding by
default; send `compact=0` to request the original BEP 3 list of peer
dictionaries. Compact IPv4 and IPv6 peers are returned under `peers` and
`peers6`, respectively.

Calling `/scrape` without an `info_hash` returns a full scrape. To request only
specific torrents, repeat the percent-encoded `info_hash` query parameter. The
response is binary bencoded tracker data (`application/x-bittorrent`), not
human-readable text; use a bencode decoder rather than viewing it directly in a
browser.

Add `format=json` to have Hive decode the same bencoded scrape payload and
return `application/json` instead:

```text
GET /scrape?format=json
GET /scrape?info_hash=%00%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F%10%11%12%13&format=json
```

JSON responses use lowercase hexadecimal info hashes as object keys:

```json
{
  "files": {
    "000102030405060708090a0b0c0d0e0f10111213": {
      "complete": 4,
      "downloaded": 12,
      "incomplete": 2
    }
  }
}
```

Omit `format` or use `format=bencode` to retain the standard BEP 48 binary
response.

## Verification

Run formatting, tests, and lint checks before deployment:

```powershell
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Load Benchmark

The ignored integration benchmark starts the release server with an isolated
SQLite database, sends real HTTP announce requests, and reports throughput,
average latency, average process CPU usage, and peak process memory. By default,
it runs 100,000 requests with concurrency set to 2,000:

```powershell
cargo test --release --test application_load -- --ignored --nocapture
```

Set environment variables to choose a different request count or concurrency:

```powershell
$env:HIVE_BENCH_REQUESTS = "100000"
$env:HIVE_BENCH_CONCURRENCY = "5000"
cargo test --release --test application_load -- --ignored --nocapture
```

The benchmark writes aggregate metrics to `benchmark-results.dat`, one row per
completed request to `benchmark-requests.dat`, and the plotting commands to
`benchmark-results.gnuplot` under `benchmark-results/`. The detailed data
contains each request's outcome, latency, cumulative throughput, server CPU,
and server memory. Generate the four-panel PNG with
`gnuplot benchmark-results/benchmark-results.gnuplot`.

Run it on an otherwise idle host and compare results from the same hardware.
The client and server share the host, so reported CPU and throughput include
local network-stack contention, while memory is sampled for the server process
only.

The **Load Benchmark** GitHub Actions workflow can also run this benchmark on
an Ubuntu runner. Start it manually from the Actions tab and set the request
count and concurrency. Its output is added to the job summary and kept as an
artifact for 30 days.