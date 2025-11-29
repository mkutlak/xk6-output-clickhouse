# xk6-output-timescaledb Analysis

This document provides a comprehensive analysis of the xk6-output-timescaledb repository to inform the implementation of a similar ClickHouse output extension.

## 1. Overall Architecture and Structure

### Repository Structure
```
xk6-output-timescaledb/
├── output.go              # Core extension implementation
├── config.go              # Configuration management
├── config_test.go         # Configuration tests
├── README.md              # Documentation
├── Dockerfile             # Container build
├── docker-compose.yml     # Development environment
├── Makefile              # Build automation
├── go.mod/go.sum         # Dependencies
├── samples/              # Example k6 scripts
│   └── http_2.js
└── grafana/              # Dashboard configurations
```

### Core Components
1. **Output Plugin**: Main extension that implements k6's output interface
2. **Configuration**: Handles multiple config sources with precedence
3. **Connection Pool**: PostgreSQL connection pooling via pgx/v5
4. **Periodic Flusher**: Batches and flushes metrics at intervals
5. **Schema Management**: Auto-creates database schema on startup

### Dependencies
```go
// Key Dependencies
github.com/jackc/pgx/v5 v5.2.0          // PostgreSQL driver with pooling
github.com/sirupsen/logrus v1.9.0       // Structured logging
go.k6.io/k6 v0.45.1                     // k6 framework
gopkg.in/guregu/null.v3 v3.3.0          // Nullable types
github.com/stretchr/testify v1.8.2      // Testing
```

## 2. Registration as a k6 Output Extension

### Registration Pattern
```go
package timescaledb

import "go.k6.io/k6/output"

func init() {
    output.RegisterExtension("timescaledb", newOutput)
}
```

**Key Points:**
- Uses `init()` function to auto-register on package import
- Registers with name `"timescaledb"` (matches CLI usage: `-o timescaledb=...`)
- Provides factory function `newOutput` that accepts `output.Params`

### Factory Function
```go
func newOutput(params output.Params) (output.Output, error) {
    configs, err := getConsolidatedConfig(
        params.JSONConfig,
        params.Environment,
        params.ConfigArgument,
    )
    if err != nil {
        return nil, fmt.Errorf("problem parsing config: %w", err)
    }

    pconf, err := pgxpool.ParseConfig(configs.URL.String)
    if err != nil {
        return nil, fmt.Errorf("TimescaleDB: Unable to parse config: %w", err)
    }

    pool, err := pgxpool.NewWithConfig(context.Background(), pconf)
    if err != nil {
        return nil, fmt.Errorf("TimescaleDB: Unable to create connection pool: %w", err)
    }

    o := Output{
        Pool:   pool,
        Config: configs,
        logger: params.Logger.WithFields(logrus.Fields{
            "output": "TimescaleDB",
        }),
    }

    return &o, nil
}
```

### Output Structure
```go
type Output struct {
    output.SampleBuffer              // Embedded buffer for collecting samples
    periodicFlusher *output.PeriodicFlusher
    Pool            *pgxpool.Pool    // Connection pool
    Config          config
    thresholds      map[string][]*dbThreshold
    logger          logrus.FieldLogger
}
```

**Interface Assertions:**
```go
var _ interface{ output.WithThresholds } = &Output{}
```

## 3. Configuration Handling

### Configuration Structure
```go
type config struct {
    // Connection URL: postgresql://[user[:password]@][netloc][:port][/dbname][?params]
    URL          null.String        `json:"url"`
    PushInterval types.NullDuration `json:"pushInterval"`
    dbName       null.String         // Extracted from URL
    addr         null.String         // Extracted from URL (host:port)
}
```

### Default Configuration
```go
func newConfig() config {
    return config{
        URL:          null.NewString("postgresql://localhost/myk6timescaleDB", false),
        PushInterval: types.NewNullDuration(time.Second, false),
        dbName:       null.NewString("myk6timescaleDB", false),
        addr:         null.NewString("postgresql://localhost", false),
    }
}
```

### Configuration Precedence (Lowest to Highest)
1. **Default values** (hardcoded in `newConfig()`)
2. **JSON configuration** (from k6 config file)
3. **Environment variables** (`K6_TIMESCALEDB_PUSH_INTERVAL`)
4. **CLI arguments** (from `-o timescaledb=...`)

### Configuration Consolidation
```go
func getConsolidatedConfig(
    jsonRawConf json.RawMessage,
    env map[string]string,
    confArg string,
) (config, error) {
    consolidatedConf := newConfig()

    // 1. Apply JSON config
    if jsonRawConf != nil {
        var jsonConf config
        if err := json.Unmarshal(jsonRawConf, &jsonConf); err != nil {
            return config{}, fmt.Errorf("problem unmarshalling JSON: %w", err)
        }
        consolidatedConf = consolidatedConf.apply(jsonConf)

        // Parse URL from JSON
        jsonURLConf, err := parseURL(consolidatedConf.URL.String)
        if err != nil {
            return config{}, fmt.Errorf("problem parsing URL: %w", err)
        }
        consolidatedConf = consolidatedConf.apply(jsonURLConf)
    }

    // 2. Apply environment variables
    envPushInterval, ok := env["K6_TIMESCALEDB_PUSH_INTERVAL"]
    if ok {
        pushInterval, err := time.ParseDuration(envPushInterval)
        if err != nil {
            return config{}, fmt.Errorf("invalid K6_TIMESCALEDB_PUSH_INTERVAL: %w", err)
        }
        consolidatedConf = consolidatedConf.apply(
            config{PushInterval: types.NewNullDuration(pushInterval, true)}
        )
    }

    // 3. Apply CLI argument (highest priority)
    if confArg != "" {
        parsedConfArg, err := parseURL(confArg)
        if err != nil {
            return config{}, fmt.Errorf("invalid config argument %q: %w", confArg, err)
        }
        consolidatedConf = consolidatedConf.apply(parsedConfArg)
    }

    return consolidatedConf, nil
}
```

### Configuration Merging Pattern
```go
func (c config) apply(modifiedConf config) config {
    if modifiedConf.URL.Valid {
        c.URL = modifiedConf.URL
    }
    if modifiedConf.PushInterval.Valid {
        c.PushInterval = modifiedConf.PushInterval
    }
    if modifiedConf.dbName.Valid {
        c.dbName = modifiedConf.dbName
    }
    if modifiedConf.addr.Valid {
        c.addr = modifiedConf.addr
    }
    return c
}
```

**Key Pattern:** Uses nullable types (`null.String`, `types.NullDuration`) to distinguish between "not set" and "set to default value"

## 4. Data Flow from k6 Metrics to TimescaleDB

### Flow Overview
```
k6 Test Script
    ↓
k6 Metrics Engine
    ↓
SampleBuffer (embedded in Output)
    ↓
PeriodicFlusher (triggers every PushInterval)
    ↓
flushMetrics()
    ↓
GetBufferedSamples()
    ↓
Transform to rows
    ↓
pgx.CopyFrom (bulk insert)
    ↓
TimescaleDB
```

### Detailed Flow

#### 1. Sample Collection
```go
type Output struct {
    output.SampleBuffer  // Provides GetBufferedSamples()
    // ...
}
```

The `SampleBuffer` is embedded, so k6 automatically populates it with metrics.

#### 2. Periodic Flushing
```go
func (o *Output) Start() error {
    // ... schema setup ...

    pf, err := output.NewPeriodicFlusher(
        time.Duration(o.Config.PushInterval.Duration),
        o.flushMetrics,
    )
    if err != nil {
        return err
    }

    o.periodicFlusher = pf
    return nil
}
```

#### 3. Metric Flushing
```go
func (o *Output) flushMetrics() {
    sampleContainers := o.GetBufferedSamples()
    start := time.Now()

    o.logger.WithField("sample-containers", len(sampleContainers)).Debug("flushMetrics: Collecting...")

    rows := [][]interface{}{}
    for _, sc := range sampleContainers {
        samples := sc.GetSamples()
        o.logger.WithField("samples", len(samples)).Debug("flushMetrics: Writing...")

        for _, s := range samples {
            tags := s.Tags.Map()
            row := []interface{}{s.Time, s.Metric.Name, s.Value, tags}
            rows = append(rows, row)
        }
    }

    // Batch update thresholds
    var batch pgx.Batch
    for _, t := range o.thresholds {
        for _, threshold := range t {
            batch.Queue(`UPDATE thresholds SET last_failed = $1 WHERE id = $2`,
                threshold.threshold.LastFailed, threshold.id)
        }
    }

    br := o.Pool.SendBatch(context.Background(), &batch)
    defer func() {
        if err := br.Close(); err != nil {
            o.logger.WithError(err).Warn("flushMetrics: Couldn't close batch results")
        }
    }()

    // Bulk insert samples
    _, err := o.Pool.CopyFrom(context.Background(),
        pgx.Identifier{"samples"},
        []string{"ts", "metric", "value", "tags"},
        pgx.CopyFromRows(rows))
    if err != nil {
        o.logger.WithError(err).Warn("copyMetrics: Couldn't commit samples")
    }

    // Execute threshold updates
    for i := 0; i < batch.Len(); i++ {
        ct, err := br.Exec()
        if err != nil {
            o.logger.WithError(err).Error("flushMetrics: Couldn't exec batch")
            return
        }
        if ct.RowsAffected() != 1 {
            o.logger.WithError(err).Error("flushMetrics: Batch did not insert")
            return
        }
    }

    t := time.Since(start)
    o.logger.WithField("time_since_start", t).Debug("flushMetrics: Samples committed!")
}
```

### Sample Structure
Each k6 sample contains:
- `Time`: timestamp
- `Metric.Name`: metric name string
- `Value`: numeric value
- `Tags`: key-value pairs

## 5. Key Interfaces and Types Used

### k6 Output Interface
```go
type Output interface {
    Description() string
    Start() error
    Stop() error
}
```

### Optional Interfaces
```go
// For threshold support
type WithThresholds interface {
    SetThresholds(thresholds map[string]metrics.Thresholds)
}
```

### k6 Types
```go
// From go.k6.io/k6/output
type Params struct {
    OutputType     string
    JSONConfig     json.RawMessage
    Environment    map[string]string
    ConfigArgument string
    Logger         logrus.FieldLogger
}

// From go.k6.io/k6/metrics
type Sample struct {
    Time   time.Time
    Metric *Metric
    Value  float64
    Tags   *SampleTags
}

type Threshold struct {
    Source           string
    LastFailed       bool
    AbortOnFail      bool
    AbortGracePeriod time.Duration
}
```

### Database Types
```go
type dbThreshold struct {
    id        int                 // Database ID
    threshold *metrics.Threshold  // k6 threshold
}
```

## 6. Batching and Buffering Strategies

### Buffer Management
- Uses **embedded `output.SampleBuffer`** from k6 framework
- k6 automatically buffers samples between flush intervals
- Buffer is cleared after each flush via `GetBufferedSamples()`

### Batch Writing

#### 1. Sample Batching
```go
// Collect all samples into rows
rows := [][]interface{}{}
for _, sc := range sampleContainers {
    samples := sc.GetSamples()
    for _, s := range samples {
        tags := s.Tags.Map()
        row := []interface{}{s.Time, s.Metric.Name, s.Value, tags}
        rows = append(rows, row)
    }
}

// Bulk insert using COPY protocol (most efficient for PostgreSQL)
_, err := o.Pool.CopyFrom(context.Background(),
    pgx.Identifier{"samples"},
    []string{"ts", "metric", "value", "tags"},
    pgx.CopyFromRows(rows))
```

**Advantages of CopyFrom:**
- Much faster than individual INSERTs
- Uses PostgreSQL COPY protocol
- Minimizes round trips

#### 2. Threshold Update Batching
```go
var batch pgx.Batch
for _, t := range o.thresholds {
    for _, threshold := range t {
        batch.Queue(`UPDATE thresholds SET last_failed = $1 WHERE id = $2`,
            threshold.threshold.LastFailed, threshold.id)
    }
}

br := o.Pool.SendBatch(context.Background(), &batch)
```

**Batching Strategy:**
- Batches all threshold updates
- Sends as single batch to minimize round trips
- Verifies each update affected exactly 1 row

### Flush Interval
- Default: **1 second** (`PushInterval`)
- Configurable via `K6_TIMESCALEDB_PUSH_INTERVAL`
- Controlled by `output.PeriodicFlusher`

## 7. Error Handling Approaches

### Principle: Non-Fatal Errors
The extension follows a "best effort" approach - errors are logged but don't crash the test.

### Error Handling Patterns

#### 1. Startup Errors (Fatal)
```go
func newOutput(params output.Params) (output.Output, error) {
    configs, err := getConsolidatedConfig(...)
    if err != nil {
        return nil, fmt.Errorf("problem parsing config: %w", err)
    }

    pool, err := pgxpool.NewWithConfig(context.Background(), pconf)
    if err != nil {
        return nil, fmt.Errorf("TimescaleDB: Unable to create connection pool: %w", err)
    }
    // ...
}
```
**Strategy:** Return errors during initialization (prevents test from running with broken output)

#### 2. Schema Setup Errors (Non-Fatal)
```go
func (o *Output) Start() error {
    _, err = conn.Exec(context.Background(), "CREATE DATABASE "+o.Config.dbName.String)
    if err != nil {
        o.logger.WithError(err).Debug("Start: Couldn't create database; most likely harmless")
    }

    _, err = conn.Exec(context.Background(), schema)
    if err != nil {
        o.logger.WithError(err).Debug("Start: Couldn't create database schema; most likely harmless")
    }
    // ...
}
```
**Strategy:** Log but continue (database/schema might already exist)

#### 3. Runtime Errors (Non-Fatal)
```go
func (o *Output) flushMetrics() {
    _, err := o.Pool.CopyFrom(...)
    if err != nil {
        o.logger.WithError(err).Warn("copyMetrics: Couldn't commit samples")
    }
    // Continues to next flush
}
```
**Strategy:** Log and continue (don't crash the test, samples are lost but test continues)

#### 4. Resource Cleanup
```go
br := o.Pool.SendBatch(context.Background(), &batch)
defer func() {
    if err := br.Close(); err != nil {
        o.logger.WithError(err).Warn("flushMetrics: Couldn't close batch results")
    }
}()
```
**Strategy:** Always clean up resources with deferred cleanup

### Logging Levels
- **Debug**: Normal operations, successful writes
- **Warn**: Non-fatal errors, failed writes
- **Error**: Serious errors (threshold updates failed)

## 8. Connection Management

### Connection Pool
```go
import "github.com/jackc/pgx/v5/pgxpool"

// Parse connection string
pconf, err := pgxpool.ParseConfig(configs.URL.String)
if err != nil {
    return nil, fmt.Errorf("TimescaleDB: Unable to parse config: %w", err)
}

// Create pool
pool, err := pgxpool.NewWithConfig(context.Background(), pconf)
if err != nil {
    return nil, fmt.Errorf("TimescaleDB: Unable to create connection pool: %w", err)
}

o := Output{
    Pool: pool,
    // ...
}
```

### Connection Pool Features
- **Automatic connection pooling** (pgxpool manages pool size)
- **Connection reuse** across flushes
- **Thread-safe** for concurrent access
- **Automatic reconnection** on connection failures

### Connection Usage Patterns

#### 1. Acquire/Release Pattern (Start)
```go
func (o *Output) Start() error {
    conn, err := o.Pool.Acquire(context.Background())
    if err != nil {
        o.logger.WithError(err).Error("Start: Couldn't acquire connection")
    }
    defer conn.Release()  // Always release back to pool

    _, err = conn.Exec(...)
    // ...
}
```

#### 2. Direct Pool Usage (Flush)
```go
func (o *Output) flushMetrics() {
    // No explicit acquire - pool methods handle it internally
    _, err := o.Pool.CopyFrom(context.Background(), ...)

    br := o.Pool.SendBatch(context.Background(), &batch)
    // ...
}
```

### Cleanup
```go
func (o *Output) Stop() error {
    o.logger.Debug("Stopping...")
    defer o.logger.Debug("Stopped!")
    o.periodicFlusher.Stop()
    o.Pool.Close()  // Close all connections in pool
    return nil
}
```

## 9. Important Code Patterns and Best Practices

### 1. Nullable Types Pattern
```go
import "gopkg.in/guregu/null.v3"

type config struct {
    URL          null.String        `json:"url"`
    PushInterval types.NullDuration `json:"pushInterval"`
}

// Check if value was explicitly set
if modifiedConf.URL.Valid {
    c.URL = modifiedConf.URL
}
```
**Why:** Distinguishes between "not set" and "set to zero/empty value"

### 2. Configuration Layering Pattern
```go
func getConsolidatedConfig(...) (config, error) {
    consolidatedConf := newConfig()  // Start with defaults
    consolidatedConf = consolidatedConf.apply(jsonConf)  // Layer 1
    consolidatedConf = consolidatedConf.apply(envConf)   // Layer 2
    consolidatedConf = consolidatedConf.apply(cliConf)   // Layer 3
    return consolidatedConf, nil
}
```
**Why:** Clear precedence, immutable-style updates

### 3. Structured Logging Pattern
```go
logger := params.Logger.WithFields(logrus.Fields{
    "output": "TimescaleDB",
})

o.logger.WithField("sample-containers", len(sampleContainers)).Debug("flushMetrics: Collecting...")
o.logger.WithField("time_since_start", t).Debug("flushMetrics: Samples committed!")
```
**Why:** Contextual information, filterable logs

### 4. Batch Operations Pattern
```go
// Collect all operations
rows := [][]interface{}{}
for _, sample := range samples {
    rows = append(rows, []interface{}{...})
}

// Execute as single batch
o.Pool.CopyFrom(..., pgx.CopyFromRows(rows))
```
**Why:** Minimize database round trips, improve performance

### 5. Embedded Interface Pattern
```go
type Output struct {
    output.SampleBuffer  // Embedded - provides GetBufferedSamples()
    // ...
}
```
**Why:** Automatic interface implementation, k6 manages the buffer

### 6. Factory Function Pattern
```go
func init() {
    output.RegisterExtension("timescaledb", newOutput)
}

func newOutput(params output.Params) (output.Output, error) {
    // Setup and validation
    return &Output{...}, nil
}
```
**Why:** Defer instantiation, allow validation before creation

### 7. Schema Initialization Pattern
```go
const schema = `
    CREATE TABLE IF NOT EXISTS samples (...);
    CREATE TABLE IF NOT EXISTS thresholds (...);
    SELECT create_hypertable('samples', 'ts');
    CREATE INDEX IF NOT EXISTS ...;
`

func (o *Output) Start() error {
    _, err = conn.Exec(context.Background(), schema)
    // Log but don't fail if schema already exists
}
```
**Why:** Idempotent, works for new and existing databases

### 8. Deferred Cleanup Pattern
```go
br := o.Pool.SendBatch(context.Background(), &batch)
defer func() {
    if err := br.Close(); err != nil {
        o.logger.WithError(err).Warn("...")
    }
}()
```
**Why:** Ensures cleanup even on early returns or panics

### 9. Error Wrapping Pattern
```go
if err != nil {
    return nil, fmt.Errorf("problem parsing config: %w", err)
}
```
**Why:** Preserves error chain, enables `errors.Is()` and `errors.As()`

### 10. Performance Monitoring Pattern
```go
func (o *Output) flushMetrics() {
    start := time.Now()
    // ... do work ...
    t := time.Since(start)
    o.logger.WithField("time_since_start", t).Debug("...")
}
```
**Why:** Track flush performance, identify bottlenecks

## Database Schema

### Samples Table
```sql
CREATE TABLE IF NOT EXISTS samples (
    ts timestamptz NOT NULL DEFAULT current_timestamp,
    metric varchar(128) NOT NULL,
    tags jsonb,
    value real
);

SELECT create_hypertable('samples', 'ts');  -- TimescaleDB hypertable
CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples (ts DESC);
```

### Thresholds Table
```sql
CREATE TABLE IF NOT EXISTS thresholds (
    id serial,
    ts timestamptz NOT NULL DEFAULT current_timestamp,
    metric varchar(128) NOT NULL,
    tags jsonb,
    threshold varchar(128) NOT NULL,
    abort_on_fail boolean DEFAULT FALSE,
    delay_abort_eval varchar(128),
    last_failed boolean DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_thresholds_ts ON thresholds (ts DESC);
```

## Key Takeaways for ClickHouse Implementation

### 1. Architecture
- Embed `output.SampleBuffer` for automatic buffer management
- Use `output.PeriodicFlusher` for timed flushes
- Implement required interfaces: `output.Output` and optionally `output.WithThresholds`

### 2. Configuration
- Support three sources: JSON, environment, CLI (with precedence)
- Use nullable types to distinguish "not set" from "zero value"
- Parse and validate connection strings carefully
- Provide sensible defaults

### 3. Connection Management
- Use connection pooling from the start
- For ClickHouse: Use `clickhouse-go/v2` with connection pool
- Handle both short-lived (setup) and long-lived (flushing) connections

### 4. Data Flow
- Collect samples from `GetBufferedSamples()`
- Transform to database-specific format
- Batch insert for performance
- Log timing information

### 5. Error Handling
- Fatal errors during initialization
- Non-fatal errors during operation (log and continue)
- Always clean up resources
- Use structured logging with context

### 6. Performance
- Batch all writes
- Use bulk insert APIs (COPY for PostgreSQL, batch insert for ClickHouse)
- Minimize round trips
- Make flush interval configurable

### 7. Schema Management
- Auto-create schema on startup
- Use `IF NOT EXISTS` for idempotency
- Consider time-series optimizations (hypertables in TimescaleDB, partitioning in ClickHouse)
- Index on timestamp column

### 8. Testing
- Test configuration precedence
- Test URL parsing
- Test error conditions (invalid config, bad URLs, etc.)
- Use table-driven tests

### 9. ClickHouse-Specific Considerations
- Use ClickHouse-specific data types (DateTime64, Float64, Map/nested for tags)
- Consider using MergeTree family engines
- Implement async inserts or batch inserts
- Handle ClickHouse's eventual consistency model
- Consider using Buffer tables for write optimization
