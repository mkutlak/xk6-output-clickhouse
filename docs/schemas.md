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

All tags stored in a `Map` column — query with `tags['name']` syntax.

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

Known tags extracted to typed columns with compression codecs. 365-day TTL for automatic cleanup.

### Tag → Column Mapping (Compatible Schema)

The compatible converter pulls known k6 tags into typed columns (some accept
aliases and are type-coerced). **Any tag not listed here is preserved in the
`extra_tags` map.** Unrecognized type coercions (a non-numeric `buildId`/`status`)
drop that single sample.

| Column              | Source tag (and aliases)        | Coercion | Default when absent              |
| ------------------- | ------------------------------- | -------- | -------------------------------- |
| `testid`            | `testid`, `test_run_id`         | string   | `default`                        |
| `build_id`          | `buildId`                       | UInt32   | process-start Unix time (non-zero) |
| `release`           | `release`                       | string   | `` (empty)                       |
| `version`           | `version`                       | string   | `` (empty)                       |
| `branch`            | `branch`                        | string   | `master`                         |
| `scenario`          | `scenario`                      | string   | `` (empty)                       |
| `name`              | `name`                          | string   | `` (empty)                       |
| `method`            | `method`                        | string   | `` (empty)                       |
| `status`            | `status`                        | UInt16   | `0`                              |
| `expected_response` | `expected_response`             | Bool (`"true"`→true, else false) | `true`           |
| `error_code`        | `error_code`                    | string   | `` (empty)                       |
| `rating`            | `rating`                        | string   | `` (empty)                       |
| `resource_type`     | `resource_type`                 | string   | `` (empty)                       |
| `ui_feature`        | `ui_feature`, `uiFeature`       | string   | `` (empty)                       |
| `check_name`        | `check` (k6 native), `check_name` | string | `` (empty)                       |
| `group_name`        | `group_name`, `group`           | string   | `` (empty)                       |
| `metric_type`       | derived from the k6 metric type | Enum8    | —                                |

> **Converter defaults vs SQL `DEFAULT`**: the SQL above shows `DEFAULT` clauses
> (e.g. `testid DEFAULT ''`, `build_id DEFAULT 0`, `branch DEFAULT 'master'`), but
> the converter always writes **explicit** values — including `testid='default'`,
> `build_id=<process-start time>`, and `branch='master'`. The SQL defaults therefore
> only apply to rows inserted by other clients. Filter dashboards on `testid='default'`
> / a non-zero `build_id`, not on `''`/`0`, for rows written by this extension.

### `metric_type` values

`metric_type` is an `Enum8` mapping the k6 metric type: `counter`=1, `gauge`=2,
`rate`=3, `trend`=4. Any unknown type falls back to `trend`. The **simple** schema
has no `metric_type` column — use the `metric` name to distinguish series there.

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
./k6 run --out "xk6-clickhouse=localhost:9000?schemaMode=compatible" script.js
```

## Custom Schema

Implement the `SchemaCreator` and `SampleConverter` interfaces:

```go
// SchemaCreator manages table schema
type SchemaCreator interface {
    CreateSchema(ctx context.Context, db *sql.DB, database, table string) error
    InsertQuery(database, table string) string
}

// SampleConverter converts k6 samples to rows
type SampleConverter interface {
    Convert(ctx context.Context, sample metrics.Sample) ([]any, error)
    Release(row []any)
}
```

Register in an `init()` function:

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
