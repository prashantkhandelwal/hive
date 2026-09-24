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
SQLite, so transfer totals can be summed across the selected period. Request
rates and lifetime counters since the current process started remain available
through Prometheus.
The trend chart uses Apache ECharts 6.1.0 loaded from jsDelivr with a pinned
version and subresource integrity hash, so chart rendering requires access to
the CDN.

Tracker population totals are maintained incrementally for constant-time
telemetry updates. Dashboard history is cached in memory until a new snapshot
is committed, tracker persistence writes only changed torrents, and full HTTP
scrape responses are cached for up to five seconds with mutation-based
invalidation.

The announce path is designed to stay bounded and in memory:

* swarm seeder and leecher totals are updated incrementally rather than
  rescanned for every announce;
* peer responses remain capped by `numwant` (at most 200);
* HTTP traffic accounting uses response size hints instead of buffering and
  copying complete response bodies;
* Prometheus traffic counters are pre-bound to avoid repeated label-map lookups
  on every request; and
* stale-peer scans run on Tokio's blocking pool so large cleanup passes do not
  occupy asynchronous request workers.

Hive deliberately retains SQLite persistence and IPv6 support rather than
copying narrower in-memory-only tracker designs. Persistence is incremental and
outside the announce path, while IPv4 and IPv6 compact peer lists are emitted
separately.

## Configuration

Hive reads configuration from `hive.toml` in the working directory.

| Setting | Default | Purpose |
| --- | --- | --- |
| `default_protocol` | `both` | Listener mode: `http`, `udp`, or `both` |
| `http_addr` | `0.0.0.0:3000` | Web UI and HTTP tracker listen address |
| `udp_addr` | `0.0.0.0:6969` | UDP tracker listen address |
| `database_path` | `hive.db` | SQLite database path |
| `announce_interval` | `1800` | Client reannounce interval in seconds |
| `peer_timeout` | `3600` | Maximum idle peer age in seconds |
| `persistence_interval` | `30` | Snapshot interval in seconds |
| `rate_limit_per_minute` | `120` | Per-source-IP request allowance |
| `max_concurrent_http_requests` | `128` | Maximum HTTP requests processed concurrently |
| `log_filter` | `hive_tracker=info` | Tracing filter and verbosity |

Use `--config path/to/config.toml` to load another file. The `--protocol`
command-line argument overrides `default_protocol` for the current process.

The HTTP concurrency limit provides overload protection while retaining
headroom above the observed throughput optimum near 64 in-flight requests.
Requests above the configured limit wait until capacity is available. The
limit applies to all HTTP routes and does not affect the UDP listener. Values
must be greater than zero.

Set `log_filter` to a tracing directive such as `hive_tracker=trace` for maximum
detail or `hive_tracker=info` for quieter operational logs. Multiple directives
can be comma-separated. Keep production deployments at `info` or quieter:
per-request debug logging is intentionally opt-in because synchronous formatting
and log output reduce announce throughput.

The dashboard, tracker endpoints, aggregate statistics, and health endpoint are
public. Per-source-IP rate limiting protects HTTP and UDP tracker traffic, and
the UDP tracker uses short-lived source-bound connection IDs.

## Linux systemd service

The included `hive-tracker.service` runs Hive as a background service under a
dynamic, unprivileged user. It stores persistent data in `/var/lib/hive`, reads
configuration from `/etc/hive/hive.toml`, and sends logs to the system journal.

Build the release binary and install the binary, configuration, and unit:

```bash
cargo build --release
sudo install -Dm755 target/release/hive-tracker /usr/local/bin/hive-tracker
sudo install -Dm644 hive.toml /etc/hive/hive.toml
sudo install -Dm644 hive-tracker.service /etc/systemd/system/hive-tracker.service
```

Set `database_path = "hive.db"` in `/etc/hive/hive.toml` to store the database
in `/var/lib/hive`. Optional environment overrides can be placed in
`/etc/hive/hive.env` using the `HIVE_*` variables listed below.

Reload systemd, enable Hive at boot, and start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hive-tracker
sudo systemctl status hive-tracker
```

Follow service logs with:

```bash
sudo journalctl -u hive-tracker -f
```

## Docker

The published image is available from Docker Hub as
`prashantkhandelwal/hive`. It runs as a non-root user, exposes the dashboard and
HTTP tracker on TCP port `3000`, exposes the UDP tracker on UDP port `6969`, and
stores SQLite data under `/data`.

Run the latest release with a persistent named volume and complete runtime
configuration:

```powershell
docker run --detach `
  --name hive `
  --restart unless-stopped `
  --pull always `
  --publish 3000:3000/tcp `
  --publish 6969:6969/udp `
  --volume hive-data:/data `
  --env "HIVE_PROTOCOL=both" `
  --env "HIVE_ENABLE_HTTP_SCRAPE=true" `
  --env "HIVE_ENABLE_UDP_SCRAPE=true" `
  --env "HIVE_HTTP_ADDR=0.0.0.0:3000" `
  --env "HIVE_UDP_ADDR=0.0.0.0:6969" `
  --env "HIVE_DATABASE_PATH=/data/hive.db" `
  --env "HIVE_ANNOUNCE_INTERVAL=1800" `
  --env "HIVE_PEER_TIMEOUT=3600" `
  --env "HIVE_PERSISTENCE_INTERVAL=30" `
  --env "HIVE_RATE_LIMIT_PER_MINUTE=500" `
  --env "HIVE_MAX_CONCURRENT_HTTP_REQUESTS=128" `
  --env "HIVE_LOG_FILTER=hive_tracker=info" `
  prashantkhandelwal/hive:latest
```

Environment variables override values from `/etc/hive/hive.toml`:

| Variable | Example | Purpose |
| --- | --- | --- |
| `HIVE_PROTOCOL` | `both` | Tracker listeners: `http`, `udp`, or `both` |
| `HIVE_ENABLE_HTTP_SCRAPE` | `true` | Enable the HTTP scrape endpoint |
| `HIVE_ENABLE_UDP_SCRAPE` | `true` | Enable the UDP scrape action |
| `HIVE_HTTP_ADDR` | `0.0.0.0:3000` | HTTP listen address inside the container |
| `HIVE_UDP_ADDR` | `0.0.0.0:6969` | UDP listen address inside the container |
| `HIVE_DATABASE_PATH` | `/data/hive.db` | SQLite database path |
| `HIVE_ANNOUNCE_INTERVAL` | `1800` | Client reannounce interval in seconds |
| `HIVE_PEER_TIMEOUT` | `3600` | Maximum idle peer age in seconds |
| `HIVE_PERSISTENCE_INTERVAL` | `30` | Persistence interval in seconds |
| `HIVE_RATE_LIMIT_PER_MINUTE` | `120` | Per-source-IP request allowance |
| `HIVE_MAX_CONCURRENT_HTTP_REQUESTS` | `128` | Maximum HTTP requests processed concurrently |
| `HIVE_LOG_FILTER` | `hive_tracker=info` | Tracing filter |

To keep reusable container settings outside shell history, copy `.env.example`
to `.env`, update its values, and run:

```powershell
docker run --detach `
  --name hive `
  --restart unless-stopped `
  --publish 3000:3000/tcp `
  --publish 6969:6969/udp `
  --volume hive-data:/data `
  --env-file .env `
  prashantkhandelwal/hive:latest
```

Alternatively, use the included Compose configuration:

```powershell
docker compose up --detach
```

Build a local image directly from the repository with:

```powershell
docker build --build-arg HIVE_VERSION=local --tag hive:local .
```

The container health check calls `http://127.0.0.1:3000/health`. Inspect it
with:

```powershell
docker inspect --format "{{.State.Health.Status}}" hive
```

The default container configuration enables both tracker protocols and both
scrape endpoints. To customize other settings, mount a configuration file at
`/etc/hive/hive.toml`:

```powershell
docker run --detach --name hive `
  --publish 3000:3000 `
  --publish 6969:6969/udp `
  --volume hive-data:/data `
  --volume "${PWD}/hive.toml:/etc/hive/hive.toml:ro" `
  prashantkhandelwal/hive:latest
```

Pushing a `v*` tag or publishing a GitHub Release triggers the **Release**
workflow, which builds and uploads the binary archives. If both events occur
for the same tag at the same time, workflow concurrency keeps only the latest
run.

Release tags publish Linux images for `amd64`, `arm64`, and `arm/v7`.
Docker images are built and published by the separate **Docker Release**
workflow; the **Release** workflow only builds and publishes binary archives.

To build an existing release that was published before the workflow trigger was
available, run **Actions → Release → Run workflow** and enter its tag. You can
also trigger it with GitHub CLI:

```powershell
gh workflow run release.yml --ref main --field tag=v1.2.3
gh run watch
```

To publish an existing release tag manually, open **Actions → Docker Release →
Run workflow** and enter a tag such as `v1.2.3`. The tag must already exist in
the repository. You can also trigger it with GitHub CLI:

```powershell
gh workflow run docker-release.yml --ref main --field tag=v1.2.3
gh run watch
```

To enable release publishing, add these GitHub Actions repository secrets:

| Secret | Value |
| --- | --- |
| `DOCKERHUB_USERNAME` | Docker Hub user that can push to `prashantkhandelwal/hive` |
| `DOCKERHUB_TOKEN` | Docker Hub access token with read/write permission |

Create `prashantkhandelwal/hive` as a public Docker Hub repository before the
first release. Create the access token in Docker Hub account settings. Do not
use or commit the Docker Hub account password.

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

### Concurrency sweep and charts

The benchmark runner scripts execute multiple concurrency levels, preserve the
raw data and detailed chart from every run, and use GNUplot to create a combined
six-panel chart for throughput, latency, CPU, memory, CPU efficiency, and
failures. Cargo and GNUplot must be available on `PATH`.

Run the default sweep from PowerShell:

```powershell
.\scripts\run-load-benchmark.ps1
```

Customize the request count, concurrency levels, repetitions, and output
directory:

```powershell
.\scripts\run-load-benchmark.ps1 `
  -Requests 500000 `
  -Concurrency 1,4,16,64,256,1000 `
  -Runs 3 `
  -OutputDirectory benchmark-results\sweep
```

Run the default sweep on Linux:

```bash
bash scripts/run-load-benchmark.sh
```

Customize it with command-line options:

```bash
bash scripts/run-load-benchmark.sh \
  --requests 500000 \
  --concurrency 1,4,16,64,256,1000 \
  --runs 3 \
  --output benchmark-results/sweep
```

The combined data, GNUplot input, and PNG are written as `summary.dat`,
`summary.gnuplot`, and `summary.png` in the selected output directory.
