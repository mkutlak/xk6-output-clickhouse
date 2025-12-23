# xk6-output-clickhouse

[![Build](https://github.com/mkutlak/xk6-output-clickhouse/actions/workflows/validate.yaml/badge.svg)](https://github.com/mkutlak/xk6-output-clickhouse/actions/workflows/validate.yaml)
[![Go Version](https://img.shields.io/badge/go-1.25+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A [k6](https://k6.io) extension for outputting load test metrics to [ClickHouse](https://clickhouse.com/).

## Why ClickHouse?

ClickHouse is an open-source columnar OLAP database designed for real-time analytics. It's an excellent choice for storing k6 metrics because:

- **Fast analytics**: Query millions of metrics in milliseconds with SQL
- **Efficient compression**: Columnar storage with compression codecs reduces storage by 50-90%
- **Familiar interface**: Standard SQL queries, Grafana integration, and native visualization tools
- **Scalable**: Handles billions of rows without breaking a sweat

## Features

- **Connection Resilience** - Automatic retry with exponential backoff and in-memory buffering during outages
- **Pluggable Schemas** - Choose between simple (flexible) or compatible (optimized) schemas, or create your own
- **TLS/SSL Support** - System CA pool, custom certificates, and mutual TLS (mTLS) authentication
- **Memory Optimized** - Object pooling and atomic counters for high-throughput metric ingestion
- **Auto Setup** - Automatically creates database and tables on first run
- **Thread Safe** - Concurrent writes with proper synchronization

## Quick Start

### Prerequisites

- [Go](https://go.dev/) 1.25+
- [xk6](https://github.com/grafana/xk6): `go install go.k6.io/xk6/cmd/xk6@latest`

### Build

```bash
xk6 build --with github.com/mkutlak/xk6-output-clickhouse@latest
```

### Run

```bash
# Start ClickHouse (if you don't have one running)
docker run -d --name clickhouse -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server

# Run your k6 test with ClickHouse output
./k6 run --out clickhouse=localhost:9000 script.js
```

## Installation

### Build with xk6

```bash
# Install xk6
go install go.k6.io/xk6/cmd/xk6@latest

# Build k6 with the ClickHouse extension
xk6 build --with github.com/mkutlak/xk6-output-clickhouse@latest
```

### From Source

```bash
git clone https://github.com/mkutlak/xk6-output-clickhouse.git
cd xk6-output-clickhouse
make build
```

### Docker

```bash
# Build the Docker image
make docker-build

# Or use the pre-built image
docker pull ghcr.io/mkutlak/xk6-output-clickhouse:latest
```

## Configuration

Configuration can be provided via environment variables, URL parameters, or JSON config in your k6 script.

**Priority order** (highest to lowest):
1. Environment variables
2. URL parameters
3. JSON config
4. Default values

### Connection Options

| Option | Environment Variable | URL Param | Default | Description |
|--------|---------------------|-----------|---------|-------------|
| `addr` | `K6_CLICKHOUSE_ADDR` | (in URL) | `localhost:9000` | ClickHouse server address (use port 9440 for TLS) |
| `user` | `K6_CLICKHOUSE_USER` | `user` | `default` | Database username |
| `password` | `K6_CLICKHOUSE_PASSWORD` | `password` | `""` | Database password |
| `database` | `K6_CLICKHOUSE_DB` | `database` | `k6` | Database name |
| `table` | `K6_CLICKHOUSE_TABLE` | `table` | `samples` | Table name |
| `pushInterval` | `K6_CLICKHOUSE_PUSH_INTERVAL` | `pushInterval` | `1s` | Flush interval (e.g., "1s", "500ms") |

### Schema Options

| Option | Environment Variable | URL Param | Default | Description |
|--------|---------------------|-----------|---------|-------------|
| `schemaMode` | `K6_CLICKHOUSE_SCHEMA_MODE` | `schemaMode` | `simple` | Schema mode: `simple` or `compatible` |
| `skipSchemaCreation` | `K6_CLICKHOUSE_SKIP_SCHEMA_CREATION` | `skipSchemaCreation` | `false` | Skip automatic database/table creation |

### Retry Options

| Option | Environment Variable | URL Param | Default | Description |
|--------|---------------------|-----------|---------|-------------|
| `retryAttempts` | `K6_CLICKHOUSE_RETRY_ATTEMPTS` | `retryAttempts` | `3` | Max retry attempts (0 to disable) |
| `retryDelay` | `K6_CLICKHOUSE_RETRY_DELAY` | `retryDelay` | `100ms` | Initial delay between retries |
| `retryMaxDelay` | `K6_CLICKHOUSE_RETRY_MAX_DELAY` | `retryMaxDelay` | `5s` | Maximum delay cap |

Retries use exponential backoff: `100ms -> 200ms -> 400ms -> ... (capped at 5s)`

### Buffer Options

| Option | Environment Variable | URL Param | Default | Description |
|--------|---------------------|-----------|---------|-------------|
| `bufferEnabled` | `K6_CLICKHOUSE_BUFFER_ENABLED` | `bufferEnabled` | `true` | Enable in-memory buffering |
| `bufferMaxSamples` | `K6_CLICKHOUSE_BUFFER_MAX_SAMPLES` | `bufferMaxSamples` | `10000` | Max samples to buffer |
| `bufferDropPolicy` | `K6_CLICKHOUSE_BUFFER_DROP_POLICY` | `bufferDropPolicy` | `oldest` | Overflow policy: `oldest` or `newest` |

### TLS Options

| Option | Environment Variable | URL Param | Default | Description |
|--------|---------------------|-----------|---------|-------------|
| `tls.enabled` | `K6_CLICKHOUSE_TLS_ENABLED` | `tlsEnabled` | `false` | Enable TLS/SSL |
| `tls.insecureSkipVerify` | `K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY` | `tlsInsecureSkipVerify` | `false` | Skip cert verification (testing only) |
| `tls.caFile` | `K6_CLICKHOUSE_TLS_CA_FILE` | `tlsCAFile` | `""` | CA certificate file path |
| `tls.certFile` | `K6_CLICKHOUSE_TLS_CERT_FILE` | `tlsCertFile` | `""` | Client certificate for mTLS |
| `tls.keyFile` | `K6_CLICKHOUSE_TLS_KEY_FILE` | `tlsKeyFile` | `""` | Client key for mTLS |
| `tls.serverName` | `K6_CLICKHOUSE_TLS_SERVER_NAME` | `tlsServerName` | `""` | Server name for SNI |

## Usage Examples

### Basic Usage

```bash
./k6 run --out clickhouse=localhost:9000 script.js
```

### With Custom Database

```bash
./k6 run --out "clickhouse=localhost:9000?database=perf_tests&table=results" script.js
```

### With Authentication

```bash
./k6 run --out "clickhouse=localhost:9000?user=k6user&password=secret" script.js
```

### With Environment Variables

```bash
export K6_CLICKHOUSE_ADDR=clickhouse.example.com:9440
export K6_CLICKHOUSE_DB=k6_metrics
export K6_CLICKHOUSE_TLS_ENABLED=true
export K6_CLICKHOUSE_USER=k6user
export K6_CLICKHOUSE_PASSWORD=secret
./k6 run --out clickhouse script.js
```

### Full JSON Configuration

```javascript
export const options = {
    ext: {
        clickhouse: {
            // Connection
            addr: "clickhouse.example.com:9440",
            user: "k6user",
            password: "secret",
            database: "k6_tests",
            table: "metrics",

            // Timing
            pushInterval: "1s",

            // Schema
            schemaMode: "simple",
            skipSchemaCreation: false,

            // Retry
            retryAttempts: 5,
            retryDelay: "100ms",
            retryMaxDelay: "10s",

            // Buffer
            bufferEnabled: true,
            bufferMaxSamples: 50000,
            bufferDropPolicy: "oldest",

            // TLS
            tls: {
                enabled: true,
                caFile: "/path/to/ca.pem",
                certFile: "/path/to/cert.pem",
                keyFile: "/path/to/key.pem"
            }
        }
    },
    scenarios: {
        load_test: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '30s', target: 100 },
                { duration: '1m', target: 100 },
                { duration: '30s', target: 0 }
            ]
        }
    }
};

import http from 'k6/http';
import { sleep } from 'k6';

export default function () {
    http.get('https://test.k6.io');
    sleep(1);
}
```

## Schema System

The extension supports pluggable schemas for different use cases.

### Simple Schema (Default)

Best for: Flexible data, quick setup, all tag values preserved

```sql
CREATE TABLE k6.samples (
    timestamp DateTime64(3),
    metric LowCardinality(String),
    value Float64,
    tags Map(String, String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (metric, timestamp)
```

**Characteristics:**
- 4 columns, simple structure
- All tags stored in Map column
- Query tags with `tags['name']` syntax

### Compatible Schema

Best for: Structured data, typed columns, better compression, complex analytics

```sql
CREATE TABLE k6.samples (
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    metric LowCardinality(String),
    metric_type Enum8('counter'=1, 'gauge'=2, 'rate'=3, 'trend'=4),
    value Float64 CODEC(Gorilla, ZSTD(1)),
    testid LowCardinality(String) DEFAULT '',
    release LowCardinality(String) DEFAULT '',
    scenario LowCardinality(String) DEFAULT '',
    build_id UInt32 DEFAULT 0 CODEC(Delta, ZSTD(1)),
    version LowCardinality(String) DEFAULT '',
    branch LowCardinality(String) DEFAULT 'master',
    name String DEFAULT '' CODEC(ZSTD(1)),
    method LowCardinality(String) DEFAULT '',
    status UInt16 DEFAULT 0,
    expected_response Bool DEFAULT true,
    error_code LowCardinality(String) DEFAULT '',
    rating LowCardinality(String) DEFAULT '',
    resource_type LowCardinality(String) DEFAULT '',
    ui_feature LowCardinality(String) DEFAULT '',
    check_name String DEFAULT '' CODEC(ZSTD(1)),
    group_name LowCardinality(String) DEFAULT '',
    extra_tags Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (metric, testid, release, timestamp)
TTL toDateTime(timestamp) + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192
```

**Characteristics:**
- 21 columns with typed fields
- Known tags extracted to dedicated columns
- Compression codecs for better storage
- 365-day TTL for automatic cleanup

### Schema Comparison

| Feature | Simple | Compatible |
|---------|--------|-----------|
| Columns | 4 | 21 |
| Tag storage | All in Map | Extracted + extra_tags |
| Compression | Default | CODEC chains |
| TTL | None | 365 days |
| Query style | `tags['method']` | `method` |
| Best for | Flexibility | Analytics |

Use `schemaMode=compatible` for the compatible schema:

```bash
./k6 run --out "clickhouse=localhost:9000?schemaMode=compatible" script.js
```

## Custom Schema Implementation

Create your own schema by implementing two interfaces:

```go
// SchemaCreator manages table schema
type SchemaCreator interface {
    CreateSchema(ctx context.Context, db *sql.DB, database, table string) error
    InsertQuery(database, table string) string
    ColumnCount() int
}

// SampleConverter converts k6 samples to rows
type SampleConverter interface {
    Convert(ctx context.Context, sample metrics.Sample) ([]any, error)
    Release(row []any)
}
```

<details>
<summary><b>Complete Custom Schema Example</b></summary>

```go
package clickhouse

import (
    "context"
    "database/sql"
    "fmt"
    "strconv"

    "go.k6.io/k6/metrics"
)

func init() {
    RegisterSchema(SchemaImplementation{
        Name:      "custom",
        Schema:    MyCustomSchema{},
        Converter: MyCustomConverter{},
    })
}

type MyCustomSchema struct{}

func (s MyCustomSchema) CreateSchema(ctx context.Context, db *sql.DB, database, table string) error {
    // Create database
    _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
    if err != nil {
        return err
    }

    // Create table with your custom columns
    query := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s.%s (
            timestamp DateTime64(3),
            metric LowCardinality(String),
            value Float64,
            testid LowCardinality(String),
            scenario LowCardinality(String),
            extra_tags Map(String, String)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (metric, testid, timestamp)
    `, database, table)

    _, err = db.ExecContext(ctx, query)
    return err
}

func (s MyCustomSchema) InsertQuery(database, table string) string {
    return fmt.Sprintf(
        "INSERT INTO %s.%s (timestamp, metric, value, testid, scenario, extra_tags) VALUES (?, ?, ?, ?, ?, ?)",
        database, table)
}

func (s MyCustomSchema) ColumnCount() int { return 6 }

type MyCustomConverter struct{}

func (c MyCustomConverter) Convert(ctx context.Context, sample metrics.Sample) ([]any, error) {
    tags := make(map[string]string)
    var testid, scenario string

    if sample.Tags != nil {
        for k, v := range sample.Tags.Map() {
            switch k {
            case "testid":
                testid = v
            case "scenario":
                scenario = v
            default:
                tags[k] = v
            }
        }
    }

    return []any{
        sample.Time,
        sample.Metric.Name,
        sample.Value,
        testid,
        scenario,
        tags,
    }, nil
}

func (c MyCustomConverter) Release(row []any) {
    // Return pooled resources if using sync.Pool
}
```

Build and use:

```bash
xk6 build --with github.com/yourorg/xk6-output-clickhouse
K6_CLICKHOUSE_SCHEMA_MODE=custom ./k6 run script.js
```

</details>

## Local Development

### Using Docker Compose

Start ClickHouse for local development:

```bash
# Start ClickHouse
docker compose up -d clickhouse

# ClickHouse is available at:
# - Native protocol: localhost:9000
# - HTTP interface: localhost:8123
```

Run a test:

```bash
# Build k6 with the extension
make build

# Run a test
./k6 run --out clickhouse=localhost:9000 examples/simple.js
```

<details>
<summary><b>Enable Grafana (Optional)</b></summary>

Uncomment the Grafana service in `docker-compose.yml` and run:

```bash
docker compose up -d clickhouse grafana
# Grafana available at: http://localhost:3000 (admin/admin)
```

</details>

### Make Commands

```bash
make build            # Build k6 with extension
make test             # Run tests
make test-coverage    # Run tests with coverage
make lint             # Run golangci-lint
make check            # Run all checks (fmt, vet, tidy, test)
make docker-build     # Build Docker image
make docker-compose-up    # Start services
make docker-compose-test  # Run integration test
make help             # Show all commands
```

## Grafana Integration

Connect Grafana to ClickHouse to visualize your k6 metrics.

### Add ClickHouse Data Source

1. In Grafana, go to **Configuration > Data Sources**
2. Click **Add data source** and select **ClickHouse**
3. Configure:
   - **Server address**: `clickhouse:9000` (or your host)
   - **Server port**: `9000`
   - **Database**: `k6`
   - **Username**: `default`

### Example Dashboard Queries

**Requests per second:**
```sql
SELECT
    toStartOfInterval(timestamp, INTERVAL 1 second) AS time,
    count() AS rps
FROM k6.samples
WHERE metric = 'http_reqs'
GROUP BY time
ORDER BY time
```

**Response time percentiles:**
```sql
SELECT
    toStartOfInterval(timestamp, INTERVAL 10 second) AS time,
    quantile(0.50)(value) AS p50,
    quantile(0.90)(value) AS p90,
    quantile(0.99)(value) AS p99
FROM k6.samples
WHERE metric = 'http_req_duration'
GROUP BY time
ORDER BY time
```

**Error rate:**
```sql
SELECT
    toStartOfInterval(timestamp, INTERVAL 10 second) AS time,
    countIf(tags['status'] NOT IN ('200', '201', '204')) / count() * 100 AS error_rate
FROM k6.samples
WHERE metric = 'http_reqs'
GROUP BY time
ORDER BY time
```

## Querying Metrics

### Basic Queries

```sql
-- View recent metrics
SELECT * FROM k6.samples
ORDER BY timestamp DESC
LIMIT 100;

-- Count metrics by type
SELECT metric, count() AS cnt
FROM k6.samples
GROUP BY metric
ORDER BY cnt DESC;

-- Average response time
SELECT avg(value) AS avg_duration
FROM k6.samples
WHERE metric = 'http_req_duration';
```

### Time Series Analysis

```sql
-- Requests over time (1-minute buckets)
SELECT
    toStartOfMinute(timestamp) AS time,
    count() AS requests
FROM k6.samples
WHERE metric = 'http_reqs'
GROUP BY time
ORDER BY time;

-- Response time trends
SELECT
    toStartOfMinute(timestamp) AS time,
    avg(value) AS avg_ms,
    max(value) AS max_ms,
    min(value) AS min_ms
FROM k6.samples
WHERE metric = 'http_req_duration'
GROUP BY time
ORDER BY time;
```

### Using Tags (Simple Schema)

```sql
-- Filter by endpoint
SELECT avg(value)
FROM k6.samples
WHERE metric = 'http_req_duration'
  AND tags['name'] = '/api/users';

-- Group by HTTP method
SELECT
    tags['method'] AS method,
    count() AS requests,
    avg(value) AS avg_duration
FROM k6.samples
WHERE metric = 'http_req_duration'
GROUP BY method;
```

## Troubleshooting

<details>
<summary><b>Connection refused</b></summary>

**Symptoms:** `dial tcp: connect: connection refused`

**Solutions:**
1. Ensure ClickHouse is running: `docker ps | grep clickhouse`
2. Check the port (default: 9000 for native, 9440 for TLS)
3. Verify firewall rules allow the connection
4. For Docker, use `host.docker.internal` instead of `localhost`

</details>

<details>
<summary><b>Authentication failed</b></summary>

**Symptoms:** `authentication failed`

**Solutions:**
1. Verify username and password
2. Check ClickHouse user configuration in `users.xml`
3. Ensure the user has permissions on the database

</details>

<details>
<summary><b>TLS handshake error</b></summary>

**Symptoms:** `tls: handshake failure` or certificate errors

**Solutions:**
1. Verify you're using the correct port (9440 for TLS, not 9000)
2. Check CA certificate path and permissions
3. For self-signed certs, ensure CA is provided via `tlsCAFile`
4. Use `tlsInsecureSkipVerify=true` for testing only

</details>

<details>
<summary><b>Buffer overflow warnings</b></summary>

**Symptoms:** `buffer overflow, dropping samples` in logs

**Solutions:**
1. Increase `bufferMaxSamples` (default: 10000)
2. Reduce `pushInterval` to flush more frequently
3. Check ClickHouse server load and connection
4. Consider `bufferDropPolicy=newest` to preserve older data

</details>

<details>
<summary><b>High memory usage</b></summary>

**Symptoms:** k6 process using excessive memory

**Solutions:**
1. Reduce `bufferMaxSamples`
2. Decrease `pushInterval` to flush more often
3. Use the `simple` schema (fewer allocations than `compatible`)

</details>

## Contributing

1. Fork the repository
2. Create your feature branch: `git checkout -b feature/my-feature`
3. Make changes and add tests
4. Run checks: `make check`
5. Commit: `git commit -m 'Add my feature'`
6. Push: `git push origin feature/my-feature`
7. Open a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/xk6-output-clickhouse.git
cd xk6-output-clickhouse

# Install tools
make install-tools

# Run all checks
make check

# Build and test
make build
./k6 run --out clickhouse=localhost:9000 examples/simple.js
```

## License

[Apache 2.0](LICENSE)
