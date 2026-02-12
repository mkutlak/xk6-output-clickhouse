# xk6-output-clickhouse

[![Build](https://github.com/mkutlak/xk6-output-clickhouse/actions/workflows/validate.yaml/badge.svg)](https://github.com/mkutlak/xk6-output-clickhouse/actions/workflows/validate.yaml)
[![Go Version](https://img.shields.io/badge/go-1.25+-00ADD8?logo=go)](https://go.dev/)
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
go install go.k6.io/xk6/cmd/xk6@latest
xk6 build --with github.com/mkutlak/xk6-output-clickhouse@latest
```

### Run

```bash
# Start ClickHouse
docker run -d --name clickhouse -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server

# Run k6
./k6 run --out clickhouse=localhost:9000 script.js
```

## Documentation

- [Configuration](./docs/configuration.md)
- [Schema System](./docs/schemas.md)
- [Examples & Usage](./docs/examples.md)
- [Development & Contributing](./docs/development.md)

## License

[Apache 2.0](LICENSE)
