# Schema System

The extension supports pluggable schemas for different use cases.

## Simple Schema (Default)

Best for: Flexible data, quick setup, all tag values preserved.

```sql
CREATE TABLE IF NOT EXISTS k6.samples (
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
CREATE TABLE IF NOT EXISTS k6.samples (
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

### Repairing data written before 0.5.5

Compatible schema only. Two historical tag bugs left values sitting in
`extra_tags` instead of their typed column:

- Before 0.5.4, `ui_feature` was left empty and the value stored under
  `extra_tags['uiFeature']`.
- Before 0.5.5, `check_name` was left empty and the value stored under
  `extra_tags['check']`.

Both fixes shipped in code; existing rows need a one-time repair. The
`WHERE` clauses below only match rows still missing the value, so each
statement is safe to run more than once:

```sql
-- Restore ui_feature for rows written before the 0.5.4 fix
ALTER TABLE k6.samples
    UPDATE
        ui_feature = extra_tags['uiFeature'],
        extra_tags = mapFilter((k, v) -> k != 'uiFeature', extra_tags)
    WHERE ui_feature = '' AND extra_tags['uiFeature'] != '';

-- Restore check_name for rows written before the 0.5.5 fix
ALTER TABLE k6.samples
    UPDATE
        check_name = extra_tags['check'],
        extra_tags = mapFilter((k, v) -> k != 'check', extra_tags)
    WHERE check_name = '' AND extra_tags['check'] != '';
```

`ALTER TABLE ... UPDATE` is a mutation and runs asynchronously in the
background. Watch progress with:

```sql
SELECT mutation_id, command, parts_to_do, is_done, latest_fail_reason
FROM system.mutations
WHERE table = 'samples' AND is_done = 0;
```

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

Implement the single `Schema` interface (`pkg/clickhouse/registry.go`):

```go
type Schema interface {
    // CreateTable returns a CREATE TABLE IF NOT EXISTS statement for table,
    // which is passed already quoted as `database`.`table`.
    CreateTable(table string) string
    // InsertQuery returns an INSERT statement for table with one ? per column.
    InsertQuery(table string) string
    // Row converts a sample into column values, in InsertQuery column order.
    Row(sample metrics.Sample) ([]any, error)
}

func RegisterSchema(name string, s Schema)
```

Notes:

- `CreateTable` must be idempotent — use `CREATE TABLE IF NOT EXISTS`, since
  the extension runs it on every start unless `skipSchemaCreation=true`.
- `Row` must return values in the same order as the columns in `InsertQuery`.
- If your schema reads tags, `sample.Tags.Map()` returns a fresh copy you may
  freely mutate (e.g. delete keys as you extract them) without touching k6's
  internal state — but `sample.Tags` can be `nil`, so check before calling
  `Map()` (see the `tagMap` helper in `pkg/clickhouse/schema_simple.go`).

A minimal custom schema, built as its own Go module so it doesn't need to
live inside this repo:

```go
// mycustom/schema.go
package mycustom

import (
    "fmt"

    "github.com/mkutlak/xk6-output-clickhouse/pkg/clickhouse"
    "go.k6.io/k6/v2/metrics"
)

func init() {
    clickhouse.RegisterSchema("custom", schema{})
}

type schema struct{}

func (schema) CreateTable(table string) string {
    return fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            timestamp DateTime64(3),
            metric    LowCardinality(String),
            value     Float64
        ) ENGINE = MergeTree()
        ORDER BY (metric, timestamp)
    `, table)
}

func (schema) InsertQuery(table string) string {
    return fmt.Sprintf("INSERT INTO %s (timestamp, metric, value) VALUES (?, ?, ?)", table)
}

func (schema) Row(sample metrics.Sample) ([]any, error) {
    return []any{sample.Time, sample.Metric.Name, sample.Value}, nil
}
```

```go
// mycustom/go.mod
module example.com/mycustom

go 1.26

require (
    github.com/mkutlak/xk6-output-clickhouse v0.7.0
    go.k6.io/k6/v2 v2.3.0
)
```

Build both modules into one `k6` binary:

```bash
xk6 build \
    --with github.com/mkutlak/xk6-output-clickhouse@latest \
    --with example.com/mycustom=./mycustom
```

Select it with `schemaMode=custom`:

```bash
./k6 run --out "xk6-clickhouse=localhost:9000?schemaMode=custom" script.js
```

Refer to `pkg/clickhouse/schema_simple.go` or `pkg/clickhouse/schema_compat.go` for more complete implementation examples.

## Migrating from 0.6

0.7.0 replaced the two-interface `SchemaCreator`/`SampleConverter` API with
the single `Schema` interface above.

| 0.6 (old)                                                             | 0.7.0 (new)                                                                                    |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `SchemaCreator.CreateSchema(ctx, db *sql.DB, database, table string) error` | `Schema.CreateTable(table string) string` — return DDL; the extension executes it (and creates the database itself) |
| `SchemaCreator.InsertQuery(database, table string) string`             | `Schema.InsertQuery(table string) string` — `table` is now a single, already-quoted `` `database`.`table` `` argument |
| `SampleConverter.Convert(ctx, sample) ([]any, error)` + `SampleConverter.Release(row []any)` | `Schema.Row(sample metrics.Sample) ([]any, error)` — Convert and Release merged into one method; there's no separate release step |
| `RegisterSchema(SchemaImplementation{Name, Schema, Converter})`        | `RegisterSchema(name string, s Schema)` — one value implements table creation, the insert query, and row conversion |

`bufferMaxSamples` also changed meaning: it now counts individual samples
rather than k6's internal sample containers (each container previously held
around 8 samples for a typical HTTP request), so the default rose from
`10000` to `100000` to keep roughly the same real buffering capacity.

Configuration is stricter too: an unknown `--out` query parameter or JSON key
now fails at startup instead of being ignored, and the `--out` argument only
accepts the `clickhouse://` scheme (or none). See
[Configuration](./configuration.md).
