# Examples & Usage

## Example Scripts

The [`examples/`](../examples) directory contains ready-to-run k6 scripts. They
hold only the k6 *workload* — the ClickHouse output is selected with `--out` (or
`--config`/env vars), never from inside the script:

- `examples/simple.js` — a minimal workload. Run it with an explicit output:
  `./bin/k6 run --out xk6-clickhouse=localhost:9000 examples/simple.js`
- `examples/tls-config.js` + `examples/tls-config.json` — TLS/mTLS via a `--config`
  file: `./bin/k6 run --config examples/tls-config.json --out xk6-clickhouse examples/tls-config.js`

## Basic Usage

Run a k6 test with ClickHouse output using the default settings:

```bash
./k6 run --out xk6-clickhouse=localhost:9000 script.js
```

## Advanced Examples

### With Custom Database and Table

```bash
./k6 run --out "xk6-clickhouse=localhost:9000?database=perf_tests&table=results" script.js
```

### With Authentication

```bash
./k6 run --out "xk6-clickhouse=localhost:9000?user=k6user&password=secret" script.js
```

### Using Environment Variables

```bash
export K6_CLICKHOUSE_ADDR=clickhouse.example.com:9440
export K6_CLICKHOUSE_DB=k6_metrics
export K6_CLICKHOUSE_TLS_ENABLED=true
export K6_CLICKHOUSE_USER=k6user
export K6_CLICKHOUSE_PASSWORD=secret
./k6 run --out xk6-clickhouse script.js
```

## JSON Configuration

For richer configuration, put the settings in a k6 JSON config file under a
`collectors` entry keyed by the output name (`xk6-clickhouse`), then pass the
file with `--config`:

`k6-config.json`:

```json
{
  "collectors": {
    "xk6-clickhouse": {
      "addr": "clickhouse.example.com:9440",
      "user": "k6user",
      "password": "secret",
      "database": "k6_tests",
      "table": "metrics",
      "pushInterval": "1s",
      "schemaMode": "simple",
      "bufferEnabled": true,
      "tls": {
        "enabled": true,
        "caFile": "/path/to/ca.pem"
      }
    }
  }
}
```

```bash
./k6 run --config k6-config.json --out xk6-clickhouse script.js
```

## Grafana Integration

> When using the bundled `make docker-compose-up` (dev/full profiles), the
> ClickHouse data source is **auto-provisioned** from
> `grafana/provisioning/datasources/clickhouse.yml` — no manual setup needed. No
> dashboards are bundled; create your own or import a community ClickHouse dashboard.

### Add ClickHouse Data Source (manual setup)

1. In Grafana, go to **Data Sources** -> **Add data source**.
2. Select **ClickHouse**.
3. Configure the connection details (default port is `9000`).

### Common Queries

**Requests per second (RPS):**

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

## Querying Metrics

```sql
SELECT * FROM k6.samples ORDER BY timestamp DESC LIMIT 100;
SELECT metric, count() AS cnt FROM k6.samples GROUP BY metric ORDER BY cnt DESC;
```

### Filtering by Tags (Simple Schema)

```sql
SELECT avg(value) FROM k6.samples
WHERE metric = 'http_req_duration' AND tags['method'] = 'GET';

SELECT tags['status'] AS status, count() AS count
FROM k6.samples WHERE metric = 'http_reqs' GROUP BY status;
```
