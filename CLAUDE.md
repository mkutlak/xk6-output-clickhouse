<!-- Last reviewed: 2026-05-17. Re-review quarterly or after major toolchain/dependency upgrades. -->

# xk6-output-clickhouse

xk6-output-clickhouse is a k6 extension that streams load-test metrics into ClickHouse. It implements k6's `output.Output` interface and supports two schema modes (simple 4-column, compatible 21-column), retry with exponential backoff, in-memory failover buffering, TLS/mTLS, and pluggable schemas.

## Tech Stack

- **Go** — see `go.mod` for the version; compiled into a k6 binary with `xk6` (build-tool version pinned in `.xk6-version`)
- **k6** — implements the `output.Output` extension interface; the output registers as `xk6-clickhouse`
- **ClickHouse** — `clickhouse-go/v2` native driver (default native port 9000)
- **Resilience** — `avast/retry-go` for exponential-backoff retries
- **Logging** — `logrus` (logger supplied by k6 via `output.Params`)
- **Testing** — `testing` stdlib + `testcontainers-go` (integration tests require Docker)
- **Lint** — golangci-lint v2 (config in `.golangci.yml`)

## Essential Commands

Run `make help` for the full target list (Docker, release, modernize). Key commands:

```bash
# Quality gates
make check          # fmt + vet + tidy + test — run before claiming work done
make lint           # golangci-lint
make modernize      # apply Go modernizers (go fix)

# Build & test
make build          # build ./bin/k6 with the extension (uses xk6)
make test           # go test -v -race ./...
make test-coverage  # coverage report -> tests/coverage.html

# Local dev environment (ClickHouse + Grafana)
make docker-compose-up   # ClickHouse on :9000/:8123, Grafana on :3000
# docker-compose sets a password; pass it (the compose default is "password")
./bin/k6 run --out "xk6-clickhouse=localhost:9000?password=password" examples/simple.js
```

Run a single test:

```bash
go test -race -run TestName ./pkg/clickhouse/
```

## Agent Routing

- Go implementation / refactor → `executor` (sonnet)
- Go code review → `code-reviewer` (opus)
- Bug investigation → `debugger` (sonnet) first, then `executor`
- Concurrency, races, performance → `quality-reviewer` (sonnet)
- Schema / architecture design → `architect` (opus)
- Test strategy & coverage → `test-engineer` (sonnet)
- Docs (`README.md`, `docs/`) → `writer` (haiku)
- ClickHouse driver / k6 API docs → `document-specialist` (sonnet) with Context7 MCP

## Development Instructions

- Delegate specialized or tool-heavy work to the most appropriate agent (see Agent Routing); parallelize independent tasks where possible.
- Keep it simple — avoid over-engineering. Minimize new dependencies.
- Verify outcomes with evidence before claiming completion.
- If your model of the code does not reflect reality, ALWAYS ask before continuing.
- Place markdown documents in `docs/`; build binaries to `bin/` (prefer `make build`).
- Update `docs/` whenever you change how the extension is configured or used.
- Only create post-task documentation when explicitly asked — ALWAYS confirm before creating it.
- Ignore the `bin/` and `data/` directories when analyzing or searching code.

## Architecture

All source code lives in `pkg/clickhouse/`. The single `register.go` at the repo root registers the extension with k6 as `xk6-clickhouse`.

### Core Components

- **`output.go`** — Main `Output` struct implementing k6's `output.Output`. Manages DB connection, periodic flushing, retry logic, and graceful shutdown. Uses `sync.Pool` for zero-allocation row/tag map reuse.

- **`config.go`** — Hierarchical config parsing (env vars `K6_CLICKHOUSE_*` > URL params > JSON config file `collectors.xk6-clickhouse` > defaults). All config options use struct pointers to distinguish unset from false.

- **`interfaces.go`** — `SchemaCreator` (DDL + INSERT query) and `SampleConverter` (k6 sample → DB row) interfaces that make schemas pluggable.

- **`registry.go`** — Thread-safe schema registry. Custom schemas register at init time via `RegisterSchema()`.

- **`schema_simple.go`** — Default schema: `timestamp`, `metric`, `value`, `tags` (Map column). Most flexible.

- **`schema_compat.go`** — Legacy schema with 21 typed columns extracting known tags for better compression/query perf. Uses codecs (DoubleDelta, Gorilla, ZSTD) and 365-day TTL.

- **`buffer.go`** — Ring buffer for resilience during ClickHouse outages. Configurable capacity and drop policy (oldest/newest). Samples are replayed on next successful flush.

- **`helpers.go`** — Small shared helpers: k6-metric-type → ClickHouse-enum mapping, map get-and-delete utilities, and safe Unix-timestamp conversion.

### Data Flow

```text
k6 samples → AddMetricSamples → periodic flush (every PushInterval)
  → retry.Do with exponential backoff
    → BEGIN tx → Prepare INSERT → Convert samples via SampleConverter → Commit
  → on failure: push to failover buffer → retry next cycle
  → on Stop: drain buffer with fresh context, close connection
```

### Key Design Decisions

- **Object pooling** (`sync.Pool`) for tag maps and row slices to minimize GC pressure under high throughput
- **Flush mutex** prevents overlapping flushes; WaitGroup tracks in-flight flushes for clean shutdown
- **Commit errors** are treated as potential success to prevent duplicate inserts on retry
- **RWMutex** on connection state allows concurrent reads during health checks

## Testing

- Add tests for every new feature and bug fix — tests are ~50% of the codebase.
- Integration tests (`integration_test.go`) use `testcontainers-go` with a real ClickHouse container and require Docker.
- Key test files:
  - `concurrency_test.go` — race conditions, concurrent flush behavior
  - `integration_test.go` — end-to-end against real ClickHouse
  - `tls_test.go` — TLS/mTLS configuration scenarios
  - `buffer_test.go` — ring buffer FIFO ordering, overflow policies
