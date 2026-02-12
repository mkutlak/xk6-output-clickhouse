# Contributing to xk6-output-clickhouse

Thank you for your interest in contributing to `xk6-output-clickhouse`! We welcome contributions from the community to help improve this project.

## Getting Started

### Prerequisites

- [Go](https://golang.org/doc/install) (1.25 or later recommended)
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/) (for integration testing)
- [Make](https://www.gnu.org/software/make/)

### Setup

1. **Fork the repository** on GitHub.
2. **Clone your fork** locally:

    ```bash
    git clone https://github.com/YOUR_USERNAME/xk6-output-clickhouse.git
    cd xk6-output-clickhouse
    ```

3. **Install development tools** (xk6, golangci-lint):

    ```bash
    make install-tools
    ```

## Development Workflow

### Building

To build the custom k6 binary with the ClickHouse extension:

```bash
make build
```

This will produce a `./bin/k6` binary.

### Testing

We encourage Test Driven Development (TDD).

- **Run unit tests:**

  ```bash
  make test
  ```

- **Run a single test:**

  ```bash
  go test -race -run TestName ./pkg/clickhouse/
  ```

- **Run tests with coverage:**

  ```bash
  make test-coverage
  ```

### Integration Testing with Docker

To run integration tests or test manually against a local ClickHouse instance:

1. **Start ClickHouse and Grafana:**

    ```bash
    make docker-compose-up
    ```

    - ClickHouse: `http://localhost:8123`
    - Grafana: `http://localhost:3000`

2. **Run k6 tests against the containerized services:**

    ```bash
    make docker-compose-test
    ```

3. **Stop services:**

    ```bash
    make docker-compose-down
    ```

### Code Style & Quality

- **Formatting:** Run `make fmt` to format your code.
- **Linting:** Run `make lint` to check for linting errors.
- **All Checks:** Run `make check` to run formatting, vetting, tidying, and testing in one go.

Please ensure `make check` passes before submitting a pull request.

## Pull Request Process

1. Create a new branch for your feature or bug fix:

    ```bash
    git checkout -b feature/my-new-feature
    ```

2. Make your changes. Remember to add tests!
3. Run `make check` to ensure everything is in order.
4. Commit your changes. We prefer clear, descriptive commit messages.
5. Push to your fork and submit a Pull Request.

### Guidelines

- **Keep it simple:** Avoid over-engineering.
- **Tests:** Add tests for new features or bug fixes.
- **Dependencies:** Minimize new dependencies.
- **Documentation:** Update README.md if you change how the extension is configured or used.

## License

By contributing, you agree that your contributions will be licensed under the [Apache 2.0 License](LICENSE).
