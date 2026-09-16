## Overview

Hive is a compact BitTorrent tracker written in Rust. A shared DashMap-backed
swarm registry serves HTTP and BEP 15 UDP clients, while SQLite snapshots retain
peer and completion data across restarts.

The service exposes:

* `GET /announce` for compact HTTP announces
* `GET /scrape` for one or more repeated `info_hash` parameters
* `GET /health` for database-aware health checks
* `GET /stats` for aggregate JSON statistics
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

Select one listener with `--protocol`, or use `both` explicitly:

```powershell
cargo run --release -- --protocol http
cargo run --release -- --protocol udp
cargo run --release -- --protocol both
```

Without `--protocol`, Hive uses `HIVE_DEFAULT_PROTOCOL`. Its default value is
`both`, so the HTTP and UDP listeners run when neither option is configured.

Open `http://localhost:3000` for the dashboard. The default SQLite database is
created as `hive.db` in the working directory.

## Configuration

Hive reads configuration from environment variables.

| Variable | Default | Purpose |
| --- | --- | --- |
| `HIVE_DEFAULT_PROTOCOL` | `both` | Listener mode: `http`, `udp`, or `both` |
| `HIVE_HTTP_ADDR` | `[::]:3000` | HTTP listen address |
| `HIVE_UDP_ADDR` | `[::]:6969` | UDP listen address |
| `HIVE_DATABASE_PATH` | `hive.db` | SQLite database path |
| `HIVE_AUTH_TOKEN` | Empty | Optional HTTP bearer token |
| `HIVE_ANNOUNCE_INTERVAL` | `1800` | Client reannounce interval in seconds |
| `HIVE_PEER_TIMEOUT` | `3600` | Maximum idle peer age in seconds |
| `HIVE_PERSISTENCE_INTERVAL` | `30` | Snapshot interval in seconds |
| `HIVE_RATE_LIMIT_PER_MINUTE` | `120` | Per-source-IP request allowance |
| `RUST_LOG` | `hive_tracker=info` | Tracing filter |

The `--protocol` command-line argument overrides `HIVE_DEFAULT_PROTOCOL` for the
current process.

When `HIVE_AUTH_TOKEN` is set, `/announce`, `/scrape`, and `/metrics` require
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
Responses use compact peer encoding appropriate to the requesting address
family.

## Verification

Run formatting, tests, and lint checks before deployment:

```powershell
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```