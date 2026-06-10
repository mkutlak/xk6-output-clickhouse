# xk6-output-clickhouse

[![Build](https://github.com/mkutlak/xk6-output-clickhouse/actions/workflows/main.yaml/badge.svg)](https://github.com/mkutlak/xk6-output-clickhouse/actions/workflows/main.yaml)
[![Go Version](https://img.shields.io/badge/go-1.26+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A [k6](https://k6.io) extension for outputting load test metrics to [ClickHouse](https://clickhouse.com/).

## Features

- **Connection Resilience**: Automatic retry and in-memory buffering.
- **Pluggable Schemas**: Choose between `simple` or `compatible` schemas, or create your own.
- **TLS/mTLS Support**: Secure connections with certificate management.
- **Memory Optimized**: Uses object pooling for high-throughput ingestion.
- **Auto Setup**: Automatically creates database and tables.

## Quick Start

### Build

```bash
# Build with the pinned xk6 version this extension is tested against (see .xk6-version).
go install go.k6.io/xk6/cmd/xk6@v1.4.6
xk6 build --with github.com/mkutlak/xk6-output-clickhouse@latest
```

> Prefer `make build` for local development — it uses the pinned xk6 version
> automatically and writes the binary to `./bin/k6`.

### Run

```bash
# Start ClickHouse
docker run -d --name clickhouse -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server

# Run k6
./k6 run --out xk6-clickhouse=localhost:9000 script.js
```

## Compatibility

| | Requirement |
| --- | --- |
| **k6** | **v2.x** — this extension is built on `go.k6.io/k6/v2`. It is **not** compatible with k6 v1.x. |
| **Go** | 1.26+ |
| **xk6** | v1.4.6 (pinned in `.xk6-version`) |
| **ClickHouse** | native protocol (clickhouse-go/v2); tested against 26.x |

This project is pre-1.0: minor releases may include breaking changes. Pin a
released tag (e.g. `@v0.6.0`) rather than `@latest` for reproducible builds.

> **Upgrading from a build that used k6 v1.x?** Rebuild with k6 v2.x / the pinned
> xk6 above. No configuration, schema, env-var, or output-name changes are
> required — only the k6 module path changed (`go.k6.io/k6` → `go.k6.io/k6/v2`).

## Documentation

- [Configuration](./docs/configuration.md)
- [Schema System](./docs/schemas.md)
- [Examples & Usage](./docs/examples.md)
- [Development & Contributing](./docs/development.md)

## License

[Apache 2.0](LICENSE)
