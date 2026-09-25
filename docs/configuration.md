# Configuration

Priority (highest wins): env vars (`K6_CLICKHOUSE_*`) > `--out` argument >
JSON config (`collectors.xk6-clickhouse`, via `--config`) > defaults. An
empty value (empty string, JSON `null`, unset env var) leaves a
lower-priority source's value in place — it never resets a field to `""`.

An unknown `--out` query parameter or JSON key fails startup, listing valid
keys. JSON keys match case-insensitively; query parameters do not. An unknown
`K6_CLICKHOUSE_*` env var only logs a warning and is ignored.

## The `--out` argument

A ClickHouse DSN:
`[clickhouse://][user[:password]@]host:port[/database][?option=value&...]`.
A bare `host:port` is accepted (`clickhouse://` is assumed); any other scheme
is rejected. Query options override the userinfo/path for the same key.
Percent-encode reserved characters (`@ : / ? # %`) in the user name and
password (`#` → `%23`).

```bash
# bare host
--out xk6-clickhouse=localhost:9000

# full DSN: credentials + database
--out xk6-clickhouse=clickhouse://alice:s3cret@dbhost:9000/analytics

# DSN + query options
--out "xk6-clickhouse=clickhouse://dbhost:9000?schemaMode=compatible&bufferMaxSamples=50000"
```

## Options

| Group | Option | Env var | Default | Description |
| --- | --- | --- | --- | --- |
| Connection | `addr` | `K6_CLICKHOUSE_ADDR` | `localhost:9000` | Address (`host:port`); also a query/JSON key, overriding the DSN host |
| Connection | `user` | `K6_CLICKHOUSE_USER` | `default` | Username |
| Connection | `password` | `K6_CLICKHOUSE_PASSWORD` | `""` | Password |
| Connection | `database` | `K6_CLICKHOUSE_DB` | `k6` | Database name |
| Connection | `table` | `K6_CLICKHOUSE_TABLE` | `samples` | Table name |
| Connection | `pushInterval` | `K6_CLICKHOUSE_PUSH_INTERVAL` | `1s` | Flush interval (Go duration, e.g. `500ms`) |
| Schema | `schemaMode` | `K6_CLICKHOUSE_SCHEMA_MODE` | `simple` | `simple` or `compatible` ([Schema System](./schemas.md)) |
| Schema | `skipSchemaCreation` | `K6_CLICKHOUSE_SKIP_SCHEMA_CREATION` | `false` | Skip automatic `CREATE DATABASE`/`TABLE` |
| TLS | `tlsEnabled` | `K6_CLICKHOUSE_TLS_ENABLED` | `false` | Enable TLS |
| TLS | `tlsInsecureSkipVerify` | `K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY` | `false` | Skip certificate verification (testing only) |
| TLS | `tlsCAFile` | `K6_CLICKHOUSE_TLS_CA_FILE` | `""` | CA certificate file, appended to the system pool |
| TLS | `tlsCertFile` | `K6_CLICKHOUSE_TLS_CERT_FILE` | `""` | Client certificate file (mTLS, with `tlsKeyFile`) |
| TLS | `tlsKeyFile` | `K6_CLICKHOUSE_TLS_KEY_FILE` | `""` | Client key file (mTLS, with `tlsCertFile`) |
| TLS | `tlsServerName` | `K6_CLICKHOUSE_TLS_SERVER_NAME` | `""` | SNI server name |
| Retry | `retryAttempts` | `K6_CLICKHOUSE_RETRY_ATTEMPTS` | `3` | Max retries after the initial attempt (`0` disables; max `100`) |
| Retry | `retryDelay` | `K6_CLICKHOUSE_RETRY_DELAY` | `100ms` | Initial retry delay, doubled each attempt |
| Retry | `retryMaxDelay` | `K6_CLICKHOUSE_RETRY_MAX_DELAY` | `5s` | Cap on the exponential backoff delay |
| Buffer | `bufferEnabled` | `K6_CLICKHOUSE_BUFFER_ENABLED` | `true` | Keep samples from a failed flush in memory for retry |
| Buffer | `bufferMaxSamples` | `K6_CLICKHOUSE_BUFFER_MAX_SAMPLES` | `100000` | Max buffered samples |
| Buffer | `bufferDropPolicy` | `K6_CLICKHOUSE_BUFFER_DROP_POLICY` | `oldest` | Overflow policy: `oldest` or `newest` |

In JSON, TLS options may nest under a `tls` object (`enabled`,
`insecureSkipVerify`, `caFile`, `certFile`, `keyFile`, `serverName`) instead
of the flat `tlsXxx` keys — not both. Values may be a string, number, or
boolean for any option:

```json
{
  "collectors": {
    "xk6-clickhouse": {
      "addr": "dbhost:9000",
      "schemaMode": "compatible",
      "retryAttempts": 5,
      "tls": { "enabled": true, "certFile": "/certs/client.pem", "keyFile": "/certs/client.key" }
    }
  }
}
```

Equivalently via env: `K6_CLICKHOUSE_ADDR=dbhost:9000
K6_CLICKHOUSE_TLS_ENABLED=true k6 run --out xk6-clickhouse script.js`.

> With TLS enabled, use ClickHouse's native TLS port `9440`, not `9000` (a
> warning is logged otherwise).

## Notes

- **Booleans** use Go's `strconv.ParseBool` (`1`, `t`, `true`, `TRUE`, `0`,
  `f`, `false`); anything else fails at startup.
- **TLS**: cert/key/CA files without `tlsEnabled=true` are ignored (with a
  warning) — they don't implicitly enable TLS. Cert and key must be set
  together, and are read/parsed at startup, so a bad file fails before the
  test runs. `tlsInsecureSkipVerify=true` also ignores the CA file and
  `serverName`.
- **Retry**: `retryAttempts` > 100 is rejected. With retries enabled and a
  non-zero `retryDelay`, `retryMaxDelay` must be positive and `>= retryDelay`.
- **Buffer**: `bufferMaxSamples` must be positive when `bufferEnabled=true`.
  On overflow, `oldest` drops the oldest buffered samples (keeps the most
  recent); `newest` drops incoming samples instead (keeps what's buffered).

## Schema creation

The output runs `CREATE DATABASE IF NOT EXISTS` and `CREATE TABLE IF NOT
EXISTS` once at startup — create-only, it never `ALTER`s an existing table.
Changing `schemaMode` against an existing table does not migrate its columns;
point at a new table instead. With `skipSchemaCreation=true`, the database
and table must already exist with the exact columns of the selected schema
(see [Schema System](./schemas.md)).

## Delivery and buffering

Delivery is at-most-once, not exactly-once: rows only reach ClickHouse on
`Commit`, so the extension never resends a batch the server may already have,
and never writes the same sample twice. Failures can still lose samples:

- Retryable failures (connection refused/reset, timeouts, EOF, network
  errors) occur before `Commit` ships anything, so retrying is safe. They
  get exponential backoff up to `retryAttempts`; one failed row aborts the
  whole batch, retried as a unit.
- Commit errors are ambiguous (the batch may already be persisted
  server-side), so they're never retried or re-buffered — the batch is
  dropped rather than risk a duplicate.
- Conversion errors drop only the bad sample; the rest of the batch commits.
- If retries are exhausted and `bufferEnabled=true` (default), the batch is
  kept in memory (`bufferMaxSamples` cap, `bufferDropPolicy` on overflow) and
  retried first next flush; with `bufferEnabled=false` it's dropped
  immediately. Flushes run one at a time, so a slow one just delays the next
  tick rather than overlapping it. `Stop()` gives buffered samples one more
  drain attempt with a fresh 30-second deadline; anything still undrained is
  dropped.

## Observability

At `Stop()` the output logs one summary line (`ClickHouse output stopped`)
with cumulative counters `samplesProcessed`, `convertErrors`, `insertErrors`,
`retryAttempts`, `flushFailures`, `droppedSamples` — at `Warn` if anything
was dropped or failed, `Info` otherwise. Retry/buffer/drop events are also
logged as they happen (enable debug logging for per-flush detail). None of
this is exposed as k6 metrics; watch the logs, or increase
`bufferMaxSamples`/`pushInterval` if `flushFailures`/`droppedSamples` climb.
