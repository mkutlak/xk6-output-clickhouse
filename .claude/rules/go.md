---
paths:
  - "**/*.go"
---

## Go Rules

- Add tests for new features and bug fixes — tests are ~50% of the codebase.
- Run `make check` (fmt + vet + tidy + test) before claiming work done; `make lint` runs golangci-lint.
- Run a single test with `go test -race -run TestName ./pkg/clickhouse/`.
- Keep changes simple — avoid over-engineering; minimize new dependencies.
- Config fields use struct pointers to distinguish "unset" from a false/zero value (see `config.go`).
- All source lives in `pkg/clickhouse/`; `register.go` at the repo root wires the extension into k6.
