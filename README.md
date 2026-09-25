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
docker run -d --name clickhouse -p 9000:9000 -e CLICKHOUSE_PASSWORD=password clickhouse/clickhouse-server
```

Or run `make docker-compose-up` for ClickHouse plus a Grafana instance with
the datasource pre-provisioned.

### 3. Run k6 and query the data

```bash
./k6 run --out "xk6-clickhouse=clickhouse://default:password@localhost:9000" examples/simple.js

docker exec clickhouse clickhouse-client --password password -q "SELECT metric, count() FROM k6.samples GROUP BY metric"
```

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
- [Development & Contributing](./CONTRIBUTING.md)

## License

[Apache 2.0](LICENSE)
