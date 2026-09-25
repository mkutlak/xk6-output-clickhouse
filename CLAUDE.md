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
make test-unit      # go test -short -race ./... (no Docker required)
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

Public API surface: `New` (constructs the output), `Schema` (the pluggable-schema interface), and `RegisterSchema` (registers a custom schema). Everything else is internal.

### Core Components

- **`output.go`** — `clickhouseOutput` (k6 `output.Output`). `New` resolves config, schema, TLS and the quoted table once; `Start` connects, creates the database/table and starts k6's `PeriodicFlusher`; `flush` → `write` (convert once) → `insertRows` (retried); failed samples wait in `pending`, trimmed by `bound()`; `Stop` drains once and closes the connection.

- **`config.go`** — Hierarchical config parsing: JSON config (`collectors.xk6-clickhouse`) < `--out` DSN argument < `K6_CLICKHOUSE_*` environment variables (later sources override earlier ones), then `validate()`. A single `options` table lists every config key with its environment variable name; `set()` assigns a parsed value to the matching `config` field.

- **`registry.go`** — Thread-safe schema registry keyed by `schemaMode` name. The public `Schema` interface (`CreateTable`, `InsertQuery`, `Row`) makes schemas pluggable; custom schemas register at init time via `RegisterSchema()`.

- **`schema_simple.go`** — Default schema: `timestamp`, `metric`, `value`, `tags` (Map column). Most flexible.

- **`schema_compat.go`** — Legacy schema with 21 typed columns extracting known tags for better compression/query perf. Uses codecs (DoubleDelta, Gorilla, ZSTD) and 365-day TTL.

### Data Flow

```text
k6 samples → AddMetricSamples (k6's output.SampleBuffer) → PeriodicFlusher calls flush every PushInterval
  → flush: pending samples (from a prior failed flush) + newly buffered samples
    → write: convert each sample to a row once via Schema.Row
      → insertRows with retry.Do (exponential backoff): BEGIN tx → Prepare INSERT → Exec each row → Commit
  → on failure: non-commit errors are kept in pending (trimmed to BufferMaxSamples via bound()) for the next flush;
    commit errors are ambiguous (data may already be persisted) and are neither retried nor re-buffered
  → on Stop: stop the flusher, drain pending once with a fresh bounded context, close the DB connection
```

### Key Design Decisions

- **No internal locking around flush** — k6's `PeriodicFlusher` invokes `flush` on a single goroutine, one call at a time, so there is no flush mutex, WaitGroup, or RWMutex in this package
- **No object pooling** — `TagSet.Map()` already returns a fresh map per sample; rows are plain slices allocated per flush
- **Commit errors are ambiguous, not retried** — if `Commit()` fails, the server may have already persisted the data, so those samples are neither retried nor re-buffered (avoids duplicate inserts)
- **Samples are converted once per flush** — only the database write (`insertRows`) is retried; a converted row is never re-converted
- **`BufferMaxSamples` counts samples**, not bytes; `bound()` drops the oldest or newest samples per `BufferDropPolicy` once the pending buffer exceeds the limit

## Testing

- Add tests for every new feature and bug fix — tests are ~50% of the codebase.
- Integration tests (`integration_test.go`) use `testcontainers-go` with a real ClickHouse container and require Docker; run `make test-unit` to skip them.
- Key test files, e.g.:
  - `integration_test.go` — end-to-end against real ClickHouse
  - `tls_test.go` — TLS/mTLS configuration scenarios
  - `config_test.go` — config parsing and validation
  - `schema_test.go` — schema DDL/INSERT/Row conversion
  - `output_test.go` — output lifecycle and flush behavior
