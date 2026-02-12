# Examples & Usage

## Basic Usage

Run a k6 test with ClickHouse output using the default settings:

```bash
./k6 run --out clickhouse=localhost:9000 script.js
```

## Advanced Examples

### With Custom Database and Table

```bash
./k6 run --out "clickhouse=localhost:9000?database=perf_tests&table=results" script.js
```

### With Authentication

```bash
./k6 run --out "clickhouse=localhost:9000?user=k6user&password=secret" script.js
```

### Using Environment Variables

```bash
export K6_CLICKHOUSE_ADDR=clickhouse.example.com:9440
export K6_CLICKHOUSE_DB=k6_metrics
export K6_CLICKHOUSE_TLS_ENABLED=true
export K6_CLICKHOUSE_USER=k6user
export K6_CLICKHOUSE_PASSWORD=secret
./k6 run --out clickhouse script.js
```

## JSON Configuration

You can define the configuration directly in your k6 script options:

```javascript
export const options = {
  ext: {
    clickhouse: {
      addr: "clickhouse.example.com:9440",
      user: "k6user",
      password: "secret",
      database: "k6_tests",
      table: "metrics",
      pushInterval: "1s",
      schemaMode: "simple",
      bufferEnabled: true,
      tls: {
        enabled: true,
        caFile: "/path/to/ca.pem",
      },
    },
  },
};

import http from "k6/http";
import { sleep } from "k6";

export default function () {
  http.get("https://test.k6.io");
  sleep(1);
}
```

## Grafana Integration

Connect Grafana to ClickHouse to visualize your k6 metrics.

### Add ClickHouse Data Source

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

## Querying Metrics in ClickHouse

### Basic Exploration

```sql
-- View recent metrics
SELECT * FROM k6.samples ORDER BY timestamp DESC LIMIT 100;

-- Count metrics by name
SELECT metric, count() AS cnt FROM k6.samples GROUP BY metric ORDER BY cnt DESC;
```

### Using Tags (Simple Schema)

```sql
-- Filter by a specific tag
SELECT avg(value)
FROM k6.samples
WHERE metric = 'http_req_duration'
  AND tags['method'] = 'GET';

-- Group by tag
SELECT
    tags['status'] AS status,
    count() AS count
FROM k6.samples
WHERE metric = 'http_reqs'
GROUP BY status;
```
