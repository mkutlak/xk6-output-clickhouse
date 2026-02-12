# Configuration

xk6-output-clickhouse can be configured via environment variables, URL parameters, or JSON configuration in your k6 script.

## Priority Order

Highest to lowest:

1. Environment variables (`K6_CLICKHOUSE_*`)
2. URL parameters (e.g., `--out clickhouse=...?param=value`)
3. JSON configuration (`options.ext.clickhouse`)
4. Default values

## Connection Options

| Option | Environment Variable | URL Param | Default          | Description                                       |
| ------ | -------------------- | --------- | ---------------- | ------------------------------------------------- |
| `addr` | `K6_CLICKHOUSE_ADDR` | (in URL)  | `localhost:9000` | ClickHouse server address (use port 9440 for TLS) |

> **Note**: If TLS is enabled, it is recommended to use port `9440` (the default port for secure native protocol) instead of `9000`.
> | `user` | `K6_CLICKHOUSE_USER` | `user` | `default` | Database username |
> | `password` | `K6_CLICKHOUSE_PASSWORD` | `password` | `""` | Database password |
> | `database` | `K6_CLICKHOUSE_DB` | `database` | `k6` | Database name |
> | `table` | `K6_CLICKHOUSE_TABLE` | `table` | `samples` | Table name |
> | `pushInterval` | `K6_CLICKHOUSE_PUSH_INTERVAL` | `pushInterval` | `1s` | Flush interval (e.g., "1s", "500ms") |

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

Retries use exponential backoff: `100ms -> 200ms -> 400ms -> ... (capped at 5s)`

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
