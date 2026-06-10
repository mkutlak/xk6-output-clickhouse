# Development & Contributing

## Local Development

### Prerequisites

- [Go](https://go.dev/) 1.26+
- [xk6](https://github.com/grafana/xk6)
- Docker (for ClickHouse)

### Setup ClickHouse

Use the provided `docker-compose.yml` to start a local ClickHouse instance:

```bash
make docker-compose-up
```

### Build from Source

```bash
make build
```

This produces a `bin/k6` binary.

### Run Tests

```bash
make test               # Run unit tests
make test-coverage      # Run tests and generate coverage report
make docker-compose-test # Run integration tests using Docker
```

### Linting

```bash
make lint
```

## Troubleshooting

- **Connection Refused**: Ensure ClickHouse is running and reachable on the configured port.
- **Authentication Failed**: Check your username and password.
- **TLS Handshake Error**: Verify TLS configuration and port (usually 9440 for TLS).
- **Buffer Overflow**: If you see "buffer overflow, dropping samples", consider increasing `bufferMaxSamples` or decreasing `pushInterval`.

## Maintenance Scripts

The [`scripts/`](../scripts) directory holds one-off ClickHouse backfill helpers
for the **compatible** schema only (they operate on the `check_name`/`ui_feature`/
`extra_tags` columns, which the simple schema does not have). They require
`clickhouse-client` in `PATH`.

- `scripts/backfill_check.sh` — backfill `check_name` from `extra_tags['check']`.
- `scripts/backfill_ui_feature.sh` — backfill `ui_feature` from `extra_tags['uiFeature']`.

Both execute a live `ALTER TABLE ... UPDATE` mutation by default; pass `--dry-run`
to preview the affected row count and SQL without executing. Pass connection
details with `-h/-p/-u/-P/-d/-t` (against the bundled docker-compose, use
`-P password`).

## Contributing

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature/my-new-feature`.
3. Commit your changes: `git commit -am 'Add some feature'`.
4. Push to the branch: `git push origin feature/my-new-feature`.
5. Submit a pull request.
