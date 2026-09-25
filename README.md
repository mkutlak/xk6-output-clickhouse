# xk6-output-clickhouse

[![Build](https://github.com/mkutlak/xk6-output-clickhouse/actions/workflows/main.yaml/badge.svg)](https://github.com/mkutlak/xk6-output-clickhouse/actions/workflows/main.yaml)
[![Go Version](https://img.shields.io/badge/go-1.26+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A [k6](https://k6.io) extension for outputting load test metrics to [ClickHouse](https://clickhouse.com/).

## Features

- **Connection Resilience**: Automatic retry with exponential backoff and in-memory sample buffering during outages.
- **Pluggable Schemas**: Choose between `simple` or `compatible` schemas, or register your own.
- **TLS/mTLS Support**: Secure connections with certificate management.
- **Flexible Configuration**: A `clickhouse://` DSN on `--out`, env vars, or a JSON config file — unknown options fail fast at startup.
- **Auto Setup**: Automatically creates the database and table if they don't exist.

## Quick Start

### 1. Build

```bash
go install go.k6.io/xk6/cmd/xk6@latest
xk6 build --with github.com/mkutlak/xk6-output-clickhouse@latest
```

For reproducible builds, pin a released tag instead of `@latest`, and the xk6
version from [`.xk6-version`](.xk6-version) that CI tests with. From a clone,
`make build` does both and writes `./bin/k6`.

### 2. Start ClickHouse

```bash
docker run -d --name clickhouse -p 9000:9000 -e CLICKHOUSE_PASSWORD=password clickhouse/clickhouse-server:26.3-alpine
```

A password is required: the official image rejects passwordless network
logins for the `default` user.

Or run `make docker-compose-up` for ClickHouse plus a Grafana instance with
the datasource pre-provisioned.

### 3. Run k6 and query the data

```bash
./k6 run --out "xk6-clickhouse=clickhouse://default:password@localhost:9000" examples/simple.js

docker exec clickhouse clickhouse-client --password password -q "SELECT metric, count() FROM k6.samples GROUP BY metric"
```

## Prebuilt Docker image

Every release publishes `ghcr.io/mkutlak/xk6-output-clickhouse` for
`linux/amd64` and `linux/arm64`, tagged `latest`, `vX.Y.Z`, and `X.Y.Z`. Its
entrypoint is `k6`, so pass k6 arguments directly:

```bash
docker run --rm --network container:clickhouse \
  -v "$PWD/examples:/scripts:ro" \
  ghcr.io/mkutlak/xk6-output-clickhouse:latest \
  run --out "xk6-clickhouse=clickhouse://default:password@localhost:9000" /scripts/simple.js
```

This reuses the `clickhouse` container from step 2 above. The image contains
only this extension — to combine extensions, build your own k6 with `xk6`.

## Configuration

Settings layer in this order, highest wins: JSON config
(`collectors.xk6-clickhouse`) < `--out` DSN < `K6_CLICKHOUSE_*` env vars.

| Option | Env var | Default |
| --- | --- | --- |
| `addr` | `K6_CLICKHOUSE_ADDR` | `localhost:9000` |
| `user` | `K6_CLICKHOUSE_USER` | `default` |
| `password` | `K6_CLICKHOUSE_PASSWORD` | `""` |
| `database` | `K6_CLICKHOUSE_DB` | `k6` |
| `table` | `K6_CLICKHOUSE_TABLE` | `samples` |
| `pushInterval` | `K6_CLICKHOUSE_PUSH_INTERVAL` | `1s` |
| `schemaMode` | `K6_CLICKHOUSE_SCHEMA_MODE` | `simple` |
| `bufferMaxSamples` | `K6_CLICKHOUSE_BUFFER_MAX_SAMPLES` | `100000` |

```bash
--out "xk6-clickhouse=clickhouse://alice:s3cret@dbhost:9000/analytics?schemaMode=compatible"
```

Full option list, TLS/mTLS, retry and buffer tuning, and the JSON config
format: [Configuration](./docs/configuration.md).

## Choosing a schema

`simple` (default) stores every tag in a `timestamp`/`metric`/`value`/`tags`
Map — flexible, zero setup. `compatible` extracts known k6 tags into typed,
codec-compressed columns with a 365-day TTL for better compression and query
performance. Schema creation is create-only: switching `schemaMode` against
an existing table does not migrate it — point at a new table instead. See
[Schema System](./docs/schemas.md).

## Resilience

Transient failures (connection errors, timeouts) retry with exponential
backoff; if retries are exhausted, the batch moves into an in-memory buffer
(`bufferMaxSamples`), retried first on the next flush, with one final drain
attempt on `Stop()`. Rows only reach ClickHouse on `Commit`, so a sample is
never written twice — but an ambiguous commit failure is not retried and can
lose that batch. See
[Delivery and buffering](./docs/configuration.md#delivery-and-buffering).

## Troubleshooting

- **`Code: 516`, authentication failed**: the server requires a password and
  your DSN must carry one for the `default` user. Reproduced against
  `clickhouse/clickhouse-server:26.3-alpine` with no password: `Code: 516.
  DB::Exception: ... Authentication failed: password is incorrect, or there
  is no user with such name`.
- **Wrong port**: 8123 is ClickHouse's HTTP interface; this extension speaks
  the native protocol on port 9000 (9440 for TLS) — the connection error
  says so if you point it at 8123.
- **`make test` needs Docker**: it runs `testcontainers-go` integration
  tests against a real ClickHouse container. Use `make test-unit` for the
  short, Docker-free unit tests.
- **Unknown options fail fast**: an unrecognized `--out` query parameter or
  JSON key aborts startup immediately, listing every valid option.

## Compatibility

| | Requirement |
| --- | --- |
| **k6** | **v2.x** — this extension is built on `go.k6.io/k6/v2`. It is **not** compatible with k6 v1.x. |
| **Go** | 1.26+ |
| **xk6** | pinned in [`.xk6-version`](.xk6-version) |
| **ClickHouse** | native protocol (clickhouse-go/v2); tested against 26.x |

This project is pre-1.0: minor releases may include breaking changes.

Upgrading from 0.6? See [Migrating from 0.6](./docs/schemas.md#migrating-from-06):
the custom schema API changed and `bufferMaxSamples` now counts samples.

## Documentation

- [Configuration](./docs/configuration.md)
- [Schema System](./docs/schemas.md)
- [Examples & Usage](./docs/examples.md)
- [Examples](./examples/)
- [Development & Contributing](./CONTRIBUTING.md)
- [CHANGELOG](./CHANGELOG.md)

## License

[Apache 2.0](LICENSE)
