# Configuration

xk6-output-clickhouse can be configured via environment variables, URL parameters, or a JSON config file.

## Priority Order

Highest to lowest:

1. Environment variables (`K6_CLICKHOUSE_*`)
2. URL parameters (e.g., `--out xk6-clickhouse=...?param=value`)
3. JSON config file (`collectors.xk6-clickhouse` section, passed via `--config`)
4. Default values

## Connection Options

| Option | Environment Variable | URL Param | Default          | Description                                       |
| ------ | -------------------- | --------- | ---------------- | ------------------------------------------------- |
| `addr` | `K6_CLICKHOUSE_ADDR` | (positional, e.g. `--out xk6-clickhouse=host:port`) | `localhost:9000` | ClickHouse server address. Set as the positional value of the `--out` argument, not as a `?addr=` query parameter. |
| `user` | `K6_CLICKHOUSE_USER` | `user` | `default` | Database username |
| `password` | `K6_CLICKHOUSE_PASSWORD` | `password` | `""` | Database password |
| `database` | `K6_CLICKHOUSE_DB` | `database` | `k6` | Database name |
| `table` | `K6_CLICKHOUSE_TABLE` | `table` | `samples` | Table name |
| `pushInterval` | `K6_CLICKHOUSE_PUSH_INTERVAL` | `pushInterval` | `1s` | Flush interval (e.g., "1s", "500ms") |

> **Note**: With TLS enabled, use port `9440` instead of `9000`.

## Schema Options

| Option               | Environment Variable                 | URL Param            | Default  | Description                            |
| -------------------- | ------------------------------------ | -------------------- | -------- | -------------------------------------- |
| `schemaMode`         | `K6_CLICKHOUSE_SCHEMA_MODE`          | `schemaMode`         | `simple` | Schema mode: `simple` or `compatible`  |
| `skipSchemaCreation` | `K6_CLICKHOUSE_SKIP_SCHEMA_CREATION` | `skipSchemaCreation` | `false`  | Skip automatic database/table creation |

## Retry Options

| Option          | Environment Variable            | URL Param       | Default | Description                       |
| --------------- | ------------------------------- | --------------- | ------- | --------------------------------- |
| `retryAttempts` | `K6_CLICKHOUSE_RETRY_ATTEMPTS`  | `retryAttempts` | `3`     | Max retry attempts (0 to disable) |
| `retryDelay`    | `K6_CLICKHOUSE_RETRY_DELAY`     | `retryDelay`    | `100ms` | Initial delay between retries     |
| `retryMaxDelay` | `K6_CLICKHOUSE_RETRY_MAX_DELAY` | `retryMaxDelay` | `5s`    | Maximum delay cap                 |

Uses exponential backoff, capped at `retryMaxDelay`.

## Buffer Options

| Option             | Environment Variable               | URL Param          | Default  | Description                           |
| ------------------ | ---------------------------------- | ------------------ | -------- | ------------------------------------- |
| `bufferEnabled`    | `K6_CLICKHOUSE_BUFFER_ENABLED`     | `bufferEnabled`    | `true`   | Enable in-memory buffering            |
| `bufferMaxSamples` | `K6_CLICKHOUSE_BUFFER_MAX_SAMPLES` | `bufferMaxSamples` | `10000`  | Max samples to buffer                 |
| `bufferDropPolicy` | `K6_CLICKHOUSE_BUFFER_DROP_POLICY` | `bufferDropPolicy` | `oldest` | Overflow policy: `oldest` or `newest` |

## TLS Options

| Option                   | Environment Variable                     | URL Param               | Default | Description                           |
| ------------------------ | ---------------------------------------- | ----------------------- | ------- | ------------------------------------- |
| `tls.enabled`            | `K6_CLICKHOUSE_TLS_ENABLED`              | `tlsEnabled`            | `false` | Enable TLS/SSL                        |
| `tls.insecureSkipVerify` | `K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY` | `tlsInsecureSkipVerify` | `false` | Skip cert verification (testing only) |
| `tls.caFile`             | `K6_CLICKHOUSE_TLS_CA_FILE`              | `tlsCAFile`             | `""`    | CA certificate file path              |
| `tls.certFile`           | `K6_CLICKHOUSE_TLS_CERT_FILE`            | `tlsCertFile`           | `""`    | Client certificate for mTLS           |
| `tls.keyFile`            | `K6_CLICKHOUSE_TLS_KEY_FILE`             | `tlsKeyFile`            | `""`    | Client key for mTLS                   |
| `tls.serverName`         | `K6_CLICKHOUSE_TLS_SERVER_NAME`          | `tlsServerName`         | `""`    | Server name for SNI                   |

> **Boolean values**: all boolean options (`tlsEnabled`, `skipSchemaCreation`,
> `bufferEnabled`, …) are parsed with Go's `strconv.ParseBool`, so `1`, `t`,
> `true`, `TRUE`, `0`, `f`, `false` are all accepted. Any other value is rejected
> at startup with a clear error rather than being silently treated as `false`.

> **TLS material requires `tlsEnabled`**: setting `tls.caFile`/`certFile`/`keyFile`
> without enabling TLS does **not** implicitly enable it — the files are ignored
> and a warning is logged. Always set `tlsEnabled=true` (or
> `K6_CLICKHOUSE_TLS_ENABLED=true`). When `tlsInsecureSkipVerify=true`, the CA file
> and `serverName` are ignored (verification is fully disabled).

> **Retry bounds**: `retryAttempts` is capped (max 100) and rejected above that to
> avoid a misconfiguration stalling flushes and shutdown. When retries are enabled
> with a non-zero `retryDelay`, `retryMaxDelay` must be positive so exponential
> backoff stays bounded.

## Schema Creation & Migration

By default the output runs `CREATE DATABASE IF NOT EXISTS` and `CREATE TABLE IF
NOT EXISTS` on `Start()`. This is **create-only** — it never `ALTER`s an existing
table. Consequences:

- Switching `schemaMode` against a table that already exists will **not** migrate
  its columns; point the output at a new table (or drop the old one) instead.
- With `skipSchemaCreation=true`, the database and table must already exist with
  the exact columns and order of the selected schema (see [Schema System](./schemas.md)),
  or inserts will fail.

## Delivery Semantics & Resilience

Delivery is **at-least-once**, not exactly-once:

- **Retryable failures** (connection refused/reset, timeouts, EOF, network errors)
  are retried with exponential backoff up to `retryAttempts`.
- **Commit errors** are treated as ambiguous — the server may have already
  persisted the batch — so they are **not** retried and the samples are **not**
  re-buffered, to avoid duplicate inserts. A network drop between persistence and
  acknowledgement can therefore produce duplicates; de-duplicate at query time
  (e.g. with `ReplacingMergeTree` or `GROUP BY`) if exact counts matter.
- **Conversion errors** (e.g. a non-numeric `buildId`/`status` tag under the
  compatible schema) drop only the offending sample; the rest of the batch still
  commits.
- A single failed row insert aborts the **whole** current batch (which is then
  retried/buffered as a unit).

## Outage Behavior & Buffering

When `bufferEnabled=true` (default), samples from a failed flush are pushed into an
in-memory ring buffer and replayed on the next successful flush:

- **Capacity** is `bufferMaxSamples` sample containers. On overflow, `bufferDropPolicy`
  decides what to drop: `oldest` (keep the most recent data) or `newest` (keep the
  data from the start of the outage). Dropped containers are counted (see below).
- Overlapping flush cycles are skipped while a previous flush is still retrying, so
  a struggling ClickHouse is not amplified.
- On `Stop()`, the buffer is drained with a fresh 30-second deadline, retried with
  the same backoff policy as a normal flush. Anything still undrained at the end of
  that window is lost and counted as dropped.
- With `bufferEnabled=false`, samples from any failed flush are **lost immediately**
  (logged, not retried).

## Observability & Monitoring

The output maintains cumulative counters — `samplesProcessed`, `convertErrors`,
`insertErrors`, `retryAttempts`, `flushFailures`, `droppedSamples`, plus the current
`bufferedSamples` depth. These are **log-only**: a single summary line is logged at
`Stop()`, and retry/buffer/drop events are logged as they happen (enable debug
logging to see the per-flush detail). They are **not** emitted as queryable k6
metrics. Watch for `flushFailures`/`droppedSamples` climbing as the signal that
ClickHouse can't keep up — increase `bufferMaxSamples` or `pushInterval`, or fix
the connection.
