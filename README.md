# xk6-output-clickhouse

A k6 extension for outputting test metrics to ClickHouse.

## Build

Build k6 with the ClickHouse output extension:

```bash
xk6 build --with github.com/mkutlak/xk6-output-clickhouse@latest
```

## Usage

Run a k6 test and send metrics to ClickHouse:

```bash
./k6 run --out clickhouse=localhost:9000 script.js
```

### Configuration

Configure via JSON in your script:

```javascript
export const options = {
    ext: {
        clickhouse: {
            addr: "localhost:9000",
            database: "k6",
            table: "samples",
            pushInterval: "1s"
        }
    }
};
```

Or via command line:

```bash
./k6 run --out clickhouse=localhost:9000?database=k6 script.js
```

### Configuration Options

| Option | Environment Variable | Default | Description |
|--------|----------------------|---------|-------------|
| `addr` | `K6_CLICKHOUSE_ADDR` | `localhost:9000` | ClickHouse server address (use port 9440 for TLS) |
| `user` | `K6_CLICKHOUSE_USER` | `default` | Database username |
| `password` | `K6_CLICKHOUSE_PASSWORD` | `""` | Database password |
| `database` | `K6_CLICKHOUSE_DB` | `k6` | Database name |
| `table` | `K6_CLICKHOUSE_TABLE` | `samples` | Table name |
| `pushInterval` | `K6_CLICKHOUSE_PUSH_INTERVAL` | `1s` | Flush interval (e.g., "1s", "500ms") |
| `schemaMode` | `K6_CLICKHOUSE_SCHEMA_MODE` | `simple` | Schema mode: `simple` (recommended) or `compatible` (legacy) |
| `skipSchemaCreation` | `K6_CLICKHOUSE_SKIP_SCHEMA_CREATION` | `false` | Set to `true` to skip DB/Table creation |

### TLS/SSL Configuration

Secure your connection to ClickHouse using TLS. The extension supports system CA pool, custom CA certificates, and mutual TLS (mTLS) with client certificates.

#### TLS Configuration Options

| Option | Environment Variable | Default | Description |
|--------|----------------------|---------|-------------|
| `tls.enabled` | `K6_CLICKHOUSE_TLS_ENABLED` | `false` | Enable TLS/SSL connection |
| `tls.insecureSkipVerify` | `K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY` | `false` | Skip certificate verification (INSECURE - testing only) |
| `tls.caFile` | `K6_CLICKHOUSE_TLS_CA_FILE` | `""` | Path to CA certificate file (appends to system pool) |
| `tls.certFile` | `K6_CLICKHOUSE_TLS_CERT_FILE` | `""` | Path to client certificate file for mTLS |
| `tls.keyFile` | `K6_CLICKHOUSE_TLS_KEY_FILE` | `""` | Path to client private key file for mTLS |
| `tls.serverName` | `K6_CLICKHOUSE_TLS_SERVER_NAME` | `""` | Server name for SNI (Server Name Indication) |

#### Basic TLS with System CA Pool

The simplest and most secure configuration uses the system's trusted CA certificates:

```javascript
export const options = {
    ext: {
        clickhouse: {
            addr: "clickhouse.example.com:9440",
            database: "k6",
            tls: {
                enabled: true
            }
        }
    }
};
```

Command line:
```bash
./k6 run --out clickhouse=clickhouse.example.com:9440?tlsEnabled=true script.js
```

Environment variable:
```bash
export K6_CLICKHOUSE_ADDR=clickhouse.example.com:9440
export K6_CLICKHOUSE_TLS_ENABLED=true
./k6 run --out clickhouse script.js
```

#### TLS with Custom CA Certificate

If your ClickHouse server uses a self-signed certificate or private CA:

```javascript
export const options = {
    ext: {
        clickhouse: {
            addr: "clickhouse.example.com:9440",
            database: "k6",
            tls: {
                enabled: true,
                caFile: "/path/to/ca.pem"
            }
        }
    }
};
```

Command line:
```bash
./k6 run --out clickhouse=clickhouse.example.com:9440?tlsEnabled=true&tlsCAFile=/path/to/ca.pem script.js
```

#### Mutual TLS (mTLS) with Client Certificates

For environments requiring client certificate authentication:

```javascript
export const options = {
    ext: {
        clickhouse: {
            addr: "clickhouse.example.com:9440",
            database: "k6",
            tls: {
                enabled: true,
                caFile: "/path/to/ca.pem",
                certFile: "/path/to/client-cert.pem",
                keyFile: "/path/to/client-key.pem"
            }
        }
    }
};
```

Command line:
```bash
./k6 run --out clickhouse=clickhouse.example.com:9440?tlsEnabled=true&tlsCAFile=/path/to/ca.pem&tlsCertFile=/path/to/client-cert.pem&tlsKeyFile=/path/to/client-key.pem script.js
```

#### Server Name Indication (SNI)

For servers with multiple domains on the same IP:

```javascript
export const options = {
    ext: {
        clickhouse: {
            addr: "192.168.1.100:9440",
            database: "k6",
            tls: {
                enabled: true,
                serverName: "clickhouse.example.com"
            }
        }
    }
};
```

#### Important Notes

- **Default Port**: ClickHouse typically uses port `9000` for native protocol without TLS and port `9440` for native protocol with TLS. The extension will warn if you use port 9000 with TLS enabled.
- **System CA Pool**: When `tls.enabled` is `true` without specifying `caFile`, the extension uses your system's trusted CA certificates (via `x509.SystemCertPool()`).
- **Custom CA**: If you specify `caFile`, the certificate is **appended** to the system CA pool, not replaced.
- **mTLS**: Both `certFile` and `keyFile` must be specified together for client certificate authentication.
- **InsecureSkipVerify**: Only use `insecureSkipVerify: true` for testing. This disables all certificate verification and is NOT secure for production.

#### Security Best Practices

1. Always use TLS in production environments
2. Keep private keys secure with appropriate file permissions (0600)
3. Use certificate rotation and monitoring
4. Never use `insecureSkipVerify` in production
5. Validate that your ClickHouse server certificate is properly configured

## Schema

The extension creates a table with this structure:

```sql
CREATE TABLE k6.samples (
    timestamp DateTime64(3),
    metric LowCardinality(String),
    value Float64,
    tags Map(String, String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (metric, timestamp);
```

## Querying Metrics

Example queries:

```sql
-- View recent metrics
SELECT * FROM k6.samples
ORDER BY timestamp DESC
LIMIT 100;

-- Average HTTP request duration
SELECT avg(value)
FROM k6.samples
WHERE metric = 'http_req_duration';

-- Request rate over time
SELECT
    toStartOfMinute(timestamp) as time,
    count() as requests
FROM k6.samples
WHERE metric = 'http_reqs'
GROUP BY time
ORDER BY time;
```

## License

Apache 2.0
