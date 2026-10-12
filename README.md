# Mayfly

[![Test](https://github.com/ravenmk2/mayfly/actions/workflows/test.yml/badge.svg)](https://github.com/ravenmk2/mayfly/actions/workflows/test.yml) [![Release](https://img.shields.io/github/v/release/ravenmk2/mayfly)](https://github.com/ravenmk2/mayfly/releases) [![License](https://img.shields.io/github/license/ravenmk2/mayfly)](LICENSE)

Mayfly is a lightweight MQTT broker written in Go. It ships as a single static
binary (no CGO) with an embedded web console, targeting the single-node core
feature set of larger brokers.

## Features

- **Full MQTT 3.1.1 and 5.0** — QoS 0/1/2 with a complete inflight state
  machine; inbound QoS 2 is strictly exactly-once (delivery waits for PUBREL).
- **Multiple listeners** — TCP, TLS, WebSocket and WSS, with mTLS
  (`require_and_verify`) and optional certificate-CN-as-username.
- **Topic routing** — wildcard subscriptions, shared subscriptions
  (`$share/<group>/<filter>`, round-robin per group) and retained messages.
- **Persistent sessions** — clean start / session expiry, offline QoS 1/2
  queues, will messages.
- **Authentication & authorization** — separate, chained multi-source
  fall-through (file and MySQL/SQLite sources), bcrypt password hashes,
  superusers, and ACLs with `{user}` / `{clientid}` topic placeholders.
- **Persistence** — SQLite or MySQL backend with asynchronous batched writes
  and startup recovery, or run memory-only.
- **Embedded Console** — Vue 3 dashboard served from the binary: overview,
  connections, sessions, subscriptions, retained messages, user/ACL and API
  key management, test publish.
- **REST API** — full management API under `/api/v1`, authenticated with a
  console JWT or scoped API keys (`read` / `write` / `admin`).
- **Observability** — Prometheus `/metrics`, `$SYS/broker/...` retained
  topics, structured `slog` logging (text or JSON).
- **Overload protection** — connection limits, max packet size, inflight
  receive windows, bounded outbound queues, topic alias limits.
- **First-run setup wizard** — pick the primary database (SQLite or MySQL)
  and set the admin password from the browser; no restart needed. An
  environment-variable driven non-interactive mode supports automated
  deployments.

## Quick start

### Pre-built binary

Download the archive for your platform from the
[releases page](https://github.com/ravenmk2/mayfly/releases) (linux/darwin/
windows, amd64/arm64), unpack it and run:

```sh
./mayfly
```

On first run the broker starts in setup mode: open the console at
http://localhost:8080 and complete the installation wizard (primary database
selection, admin password). After setup, the MQTT listener is available on
port 1883 and all mutable state lives under `./data` (override with
`--data-dir` or `MAYFLY_DATA_DIR`).

### Docker

```sh
docker run -d --name mayfly \
  -p 1883:1883 -p 8080:8080 \
  -v mayfly-data:/data \
  ghcr.io/ravenmk2/mayfly:latest
```

The image runs as a non-root user, stores everything in the `/data` volume
and health-checks the console `/healthz` endpoint. Exposed ports: 1883
(MQTT), 8883 (MQTTS), 8083 (WS), 8084 (WSS), 8080 (console/REST/metrics).

For unattended deployments, skip the wizard via environment variables:

```sh
docker run -d --name mayfly \
  -p 1883:1883 -p 8080:8080 \
  -v mayfly-data:/data \
  -e MAYFLY_SETUP_DB=sqlite \
  -e MAYFLY_SETUP_ADMIN_PASSWORD=change-me \
  ghcr.io/ravenmk2/mayfly:latest
```

(`MAYFLY_SETUP_DB=mysql` additionally requires `MAYFLY_SETUP_MYSQL_DSN`.)

### Build from source

Requires Go (see `go.mod`) and Node.js for the console frontend:

```sh
bash scripts/build.sh   # builds web/dist and all six platform binaries into dist/
./mayfly                # or: go run ./cmd/mayfly
```

## Configuration

Configuration is a YAML file layered with environment overrides, in
decreasing precedence: CLI flags > `MAYFLY_*` environment variables >
`<data-dir>/mayfly.yaml` > built-in defaults. The file is written by the
setup wizard and can be edited by hand; changes take effect on restart.

Minimal example:

```yaml
data_dir: ./data
listeners:
  - type: tcp
    address: ":1883"
store:
  dsn: "sqlite:./data/mayfly.db"   # or "mysql:<driver-dsn>"; empty = memory-only
log:
  level: info
  format: json
admin:
  addr: ":8080"
```

Environment overrides use the `MAYFLY_` prefix with underscore-separated key
paths, e.g. `MAYFLY_LOG_LEVEL=debug` or `MAYFLY_BROKER_MAX_PACKET_SIZE=1048576`.
The full reference — listeners, TLS/mTLS, quotas, validation rules — is in
[docs/design/configuration.md](docs/design/configuration.md).

## Authentication & authorization

AuthN and AuthZ are independent chains of sources, evaluated in order with
fall-through (ordered chain). Sources can be SQL (MySQL/SQLite, managed via the
console/REST API) or YAML files. A minimal file-based setup:

```yaml
# mayfly.yaml
auth:
  anonymous_allow: false
  authentication:
    - type: file
      path: ./data/users.yaml
  authorization:
    - type: file
      path: ./data/acl.yaml
```

```yaml
# users.yaml — bcrypt password hashes
users:
  sensor1: { password: "$2y$10$...", superuser: false }
```

```yaml
# acl.yaml — first match wins; {user} / {clientid} expand per client
rules:
  - { username: "admin", action: allow, permission: pubsub,    topic: "#" }
  - { username: "*",     action: deny,  permission: pubsub,    topic: "$SYS/#" }
  - { username: "*",     action: allow, permission: publish,   topic: "up/{user}" }
  - { username: "*",     action: allow, permission: subscribe, topic: "down/{user}" }
```

Template rules like `up/{user}` keep the rule count constant regardless of
device count. See [docs/design/auth.md](docs/design/auth.md) for chain
semantics, caching and failure behavior.

## Console & setup wizard

The console is embedded into the binary and served on the admin address
(default `:8080`):

- **First run**: every page redirects to the installation wizard — choose
  SQLite (zero-config, default) or MySQL (with an in-wizard connection test),
  set the admin password, and a 32-byte master key is generated at
  `<data-dir>/master.key` (mode `0600`; back it up). The data plane
  hot-activates in-process, no restart required.
- **After setup**: dashboard with live counters, connection/session/
  subscription/retained inspection, user & ACL management, API keys and a
  test-publish page.
- **Memory-only mode** (no store configured): a random admin password is
  printed to the log once at startup.

Bind the admin listener to an internal address in production.

## REST API

All endpoints are under the admin address, JSON, `/api/v1`, authenticated via
console JWT (`POST /api/v1/login`) or `X-API-Key` header. Highlights:

| Method | Path | Description |
| --- | --- | --- |
| GET | `/api/v1/status` | version, uptime, counter summary |
| GET/DELETE | `/api/v1/connections[/{clientID}]` | list/inspect/kick connections |
| GET/DELETE | `/api/v1/sessions/{clientID}` | inspect/clear persistent sessions |
| GET | `/api/v1/subscriptions` | list subscriptions |
| GET/DELETE | `/api/v1/retained` | list/delete retained messages |
| POST | `/api/v1/publish` | server-side publish (qos/retain) |
| CRUD | `/api/v1/users`, `/api/v1/acls` | MQTT user and ACL management |
| GET/PUT | `/api/v1/auth/chains` | view/reorder auth source chains |
| CRUD | `/api/v1/apikeys` | scoped API keys (admin only) |

The complete contract (pagination, error format, scopes, setup endpoints) is
in [docs/design/admin-api.md](docs/design/admin-api.md).

## Observability

- **Prometheus**: `GET /metrics` on the admin port — connections, messages by
  QoS, bytes, subscriptions, inflight, auth/ACL results, store errors,
  goroutines, heap, `mayfly_build_info{version}`.
- **$SYS topics**: `$SYS/broker/...` published as retained messages every 10 s
  (configurable): uptime, version, client/message/subscription counters.
- **Logs**: `slog`-based, text or JSON, to stderr/stdout or a rotating file.

Details in [docs/design/observability.md](docs/design/observability.md).

## Development

```sh
go build ./...          # build
go test ./...           # unit + integration tests
golangci-lint run       # lint (or: make lint)
bash scripts/build.sh   # frontend + cross-platform release binaries into dist/
```

Frontend console (in `web/`):

```sh
cd web
npm ci
npm run dev             # vite dev server
npm test                # vitest
npm run build           # production build into web/dist (go:embed'ed)
```

## Documentation

- [docs/architecture.md](docs/architecture.md) — overall architecture, I/O
  model, key design decisions
- [docs/design/protocol.md](docs/design/protocol.md) — packet codec layer
- [docs/design/session.md](docs/design/session.md) — session lifecycle & QoS
  state machine
- [docs/design/topics.md](docs/design/topics.md) — topic tree, shared
  subscriptions, retained
- [docs/design/auth.md](docs/design/auth.md) — authentication & ACL
- [docs/design/setup.md](docs/design/setup.md) — setup wizard, data
  directory, master key
- [docs/design/persistence.md](docs/design/persistence.md) — store interface
  & async batching
- [docs/design/listeners.md](docs/design/listeners.md) — TCP/TLS/WS/WSS
  listeners
- [docs/design/configuration.md](docs/design/configuration.md) — YAML config
  & env overrides
- [docs/design/observability.md](docs/design/observability.md) — $SYS,
  Prometheus, logging
- [docs/design/admin-api.md](docs/design/admin-api.md) — REST API & console
- [docs/design/testing.md](docs/design/testing.md) — testing strategy

## License

[Apache License 2.0](LICENSE)
