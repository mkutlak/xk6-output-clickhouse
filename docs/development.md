# Development & Contributing

## Local Development

### Prerequisites

- [Go](https://go.dev/) 1.25+
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

## Contributing

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature/my-new-feature`.
3. Commit your changes: `git commit -am 'Add some feature'`.
4. Push to the branch: `git push origin feature/my-new-feature`.
5. Submit a pull request.
