English | [繁體中文](README.zh-TW.md)

# ntu-tentacle

A lightweight relay for the Nautrouds ecosystem that bridges Unix Domain Sockets (UDS) and TCP (optionally TLS) targets, letting backend services — including gRPC — be exposed over UDS with connection pooling, health probing, hot reload, and metrics reporting.

## Features

- **UDS ↔ TCP/TLS relay** — transparent byte-level forwarding (`copy_bidirectional`); it does not parse HTTP/2 or gRPC frames, so any protocol carried over the connection passes through untouched.
- **Connection pooling** — a semaphore-based pool per target caps concurrent connections without stalling new accepts.
- **Health probing** — the target TCP address is probed every 2 seconds; the UDS listener is only started while the target is reachable and is torn down when it goes offline.
- **TLS client support** — custom CA, mutual TLS (client cert/key), and ALPN fixed to `h2`/`http1.1` so gRPC (HTTP/2) negotiates correctly against upstreams that gate on ALPN.
- **Hot reload** — sending `SIGHUP` re-reads configuration and applies the new target list without dropping in-flight connections.
- **Graceful shutdown** — `SIGINT`/`SIGTERM` drain in-flight connections before the process exits.
- **Built-in metrics** — active connections, attempt/failure counters, bytes transmitted, and a latency histogram are encoded as protobuf and pushed periodically over UDS.

## Architecture

On startup, `ntu-tentacle` loads its configuration and resolves each target (including any TLS material); for every target it spawns a relay that probes the target's TCP liveness, binds the corresponding UDS, and — once a client connects — dials the TCP (or TLS-upgraded) target and forwards bytes bidirectionally, while periodically pushing metrics out over UDS as well. Sending `SIGHUP` re-applies configuration changes without interrupting existing connections.

## Installation

### Prerequisites

- Rust 1.85+ (edition 2024)
- `protoc` (Protocol Buffers compiler — required to build the `tentacle-metrics` dependency)
- Unix only

### Building from source

```bash
cargo build --release
```

### Docker

```bash
docker build -f docker/Dockerfile -t ntu-tentacle .
```

## Configuration

Configuration is read from environment variables.

| Variable | Description | Default |
|---|---|---|
| `NAUTROUDS_SERVICE_NAME` | Service name, used to build the socket directory | **required** |
| `NAUTROUDS_TARGET_ADDR` | Comma-separated list of target TCP addresses (e.g. `localhost:8080`) | **required**, unless a targets file is provided |
| `NAUTROUDS_TARGETS_FILE` | Path to a YAML targets file (see below) | none |
| `NAUTROUDS_SERVICES_DIR` | Base directory under which service socket directories are created | `/var/run/nautrouds/services` |
| `NAUTROUDS_MAX_CONNS` | Maximum concurrent connections per target | `1024` |
| `NAUTROUDS_METRICS_INTERVAL_SECS` | Interval, in seconds, between metrics pushes | `15` |
| `NAUTROUDS_PID_DIR` | Directory where the `<service_name>.pid` file is written | `/usr/local/tentacle` |

### Targets YAML file (optional, for per-target TLS and weight)

Setting a targets file **completely replaces** the target list derived from environment variables.
It is a YAML mapping of target address to an optional configuration; `cert` and `key` must both be set or both omitted — a target with only one of them set is skipped (with a warning logged) rather than failing startup.
`weight` sets the target's load-balancing weight on the nautrouds side (encoded as a `@<weight>` suffix on the socket filename); it must be an integer in `[1, 100]` and defaults to 1 when omitted.

```yaml
localhost:8080: {}

api.internal:9090:
  weight: 5
  ca: /etc/ntu-tentacle/certs/ca.pem

secure-backend:9443:
  ca: /etc/ntu-tentacle/certs/ca.pem
  cert: /etc/ntu-tentacle/certs/client.pem
  key: /etc/ntu-tentacle/certs/client.key
```

## Running

```bash
export NAUTROUDS_SERVICE_NAME=myapp
export NAUTROUDS_TARGET_ADDR=localhost:8080
./target/release/tentacle
```

## Reload

```bash
tentacle -r /usr/local/tentacle/myapp.pid   # or: --reload <path>
tentacle -r                                 # falls back to the current service's pid file, derived from the environment
```
