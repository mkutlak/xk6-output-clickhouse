# Contributing to xk6-output-clickhouse

## Prerequisites

- [Go](https://go.dev/) — version pinned in `go.mod` (1.26+)
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/) — required for integration tests and the local stack
- [xk6](https://github.com/grafana/xk6) — version pinned in `.xk6-version`; `make build` and `make install-tools` read it automatically, no manual install needed

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/xk6-output-clickhouse.git
cd xk6-output-clickhouse
make install-tools   # installs golangci-lint and xk6 at their pinned versions
```

## Building

```bash
make build   # -> ./bin/k6
```

## Testing

- `make test-unit` — short mode, no Docker required
- `make test` — full suite, including `testcontainers-go` integration tests (requires Docker)
- Single test: `go test -race -run TestName ./pkg/clickhouse/`
- `make test-coverage` — coverage report at `tests/coverage.html`

## Checks

- `make check` — fmt, vet, tidy, a `go fix -diff` modernization gate, then tests
- `make lint` — golangci-lint v2 (`make install-tools` installs the pinned version)

Both must pass before opening a pull request. Run `make help` for the full target list.

## Local stack

```bash
make docker-compose-up   # ClickHouse on :9000 (native) / :8123 (HTTP), Grafana on :3000
./bin/k6 run --out "xk6-clickhouse=localhost:9000?password=password" examples/simple.js
make docker-compose-down
```

`make docker-test` runs `examples/simple.js` against a Dockerized ClickHouse via `docker compose` and fails unless the samples actually landed in the `samples` table.

## Troubleshooting

- **Connection errors** — usually the wrong port: use the native protocol port (`9000` by default), not the HTTP port (`8123`).
- **TLS enabled on port 9000** — the extension logs a warning suggesting port `9440` for secure connections.
- **Dropped samples in the final summary** — during an outage, failed samples are kept for retry up to `bufferMaxSamples`; beyond that they are dropped per `bufferDropPolicy`. Raise `bufferMaxSamples` for more headroom.
- **`pushInterval`** — controls batch size and frequency: a longer interval means bigger, less frequent flushes.

## Commits

This project uses [Conventional Commits](https://www.conventionalcommits.org/) enforced by semantic-release: prefix commits with `feat:`, `fix:`, `docs:`, `refactor:`, etc.

While pre-1.0, breaking changes — `feat!:` or a `BREAKING CHANGE:` footer — release as a **MINOR** version (see `.releaserc.json`). This will switch to MAJOR once the project reaches 1.0.

## Pull requests

1. Create a branch, make your changes, and add tests for them.
2. Run `make check` and `make lint`; both must pass.
3. Update `docs/` if you changed configuration or usage.
4. Push and open a pull request.

Keep it simple — avoid over-engineering, and minimize new dependencies.

## License

By contributing, you agree that your contributions are licensed under the [Apache 2.0 License](LICENSE).
