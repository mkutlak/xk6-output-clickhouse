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

- `addr` - ClickHouse address (default: `localhost:9000`)
- `database` - Database name (default: `k6`)
- `table` - Table name (default: `samples`)
- `pushInterval` - How often to flush metrics (default: `1s`)

## Schema

The extension creates a table with this structure:

```sql
CREATE TABLE k6.samples (
    timestamp DateTime64(3),
    metric_name LowCardinality(String),
    metric_value Float64,
    tags Map(String, String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (metric_name, timestamp);
```

## Querying Metrics

Example queries:

```sql
-- View recent metrics
SELECT * FROM k6.samples 
ORDER BY timestamp DESC 
LIMIT 100;

-- Average HTTP request duration
SELECT avg(metric_value) 
FROM k6.samples 
WHERE metric_name = 'http_req_duration';

-- Request rate over time
SELECT 
    toStartOfMinute(timestamp) as time,
    count() as requests
FROM k6.samples 
WHERE metric_name = 'http_reqs'
GROUP BY time
ORDER BY time;
```

## License

Apache 2.0
