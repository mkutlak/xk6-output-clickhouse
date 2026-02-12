# Schema System

The extension supports pluggable schemas for different use cases.

## Simple Schema (Default)

Best for: Flexible data, quick setup, all tag values preserved.

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

- 4 columns, simple structure.
- All tags stored in `Map` column.
- Query tags with `tags['name']` syntax.

## Compatible Schema

Best for: Structured data, typed columns, better compression, complex analytics.

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

- 21 columns with typed fields.
- Known tags extracted to dedicated columns.
- Compression codecs for better storage.
- 365-day TTL for automatic cleanup.

## Schema Comparison

| Feature     | Simple           | Compatible             |
| ----------- | ---------------- | ---------------------- |
| Columns     | 4                | 21                     |
| Tag storage | All in Map       | Extracted + extra_tags |
| Compression | Default          | CODEC chains           |
| TTL         | None             | 365 days               |
| Query style | `tags['method']` | `method`               |
| Best for    | Flexibility      | Analytics              |

To use the compatible schema, set `schemaMode=compatible`:

```bash
./k6 run --out "clickhouse=localhost:9000?schemaMode=compatible" script.js
```

## Custom Schema Implementation

You can create your own schema by implementing the `SchemaCreator` and `SampleConverter` interfaces.

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

### Registration

Register your custom schema in an `init()` function:

```go
func init() {
    clickhouse.RegisterSchema(clickhouse.SchemaImplementation{
        Name:      "custom",
        Schema:    MyCustomSchema{},
        Converter: MyCustomConverter{},
    })
}
```

Refer to `pkg/clickhouse/schema_simple.go` or `pkg/clickhouse/schema_compat.go` for implementation examples.
