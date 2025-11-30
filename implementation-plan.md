# xk6-output-clickhouse Implementation Plan

## Executive Summary

This document outlines the implementation plan for **xk6-output-clickhouse**, a k6 extension that enables direct streaming of k6 performance test metrics to ClickHouse. The design is based on proven patterns from existing Grafana k6 output extensions (InfluxDB, TimescaleDB, Timestream) and optimized for ClickHouse's unique architecture.

## 1. Project Structure

```
xk6-output-clickhouse/
├── register.go                      # Extension registration entry point
├── pkg/
│   └── clickhouse/
│       ├── output.go                # Main output implementation
│       ├── output_test.go           # Unit tests for output
│       ├── config.go                # Configuration management
│       ├── config_test.go           # Configuration tests
│       └── schema.go                # Database schema management
├── go.mod                           # Go module dependencies
├── go.sum                           # Dependency checksums
├── Makefile                         # Build automation
├── Dockerfile                       # Container image for xk6 build
├── README.md                        # Documentation
├── LICENSE                          # License file
├── examples/                        # Example k6 scripts
│   ├── simple.js                    # Basic usage example
│   ├── advanced.js                  # Advanced configuration
│   └── dashboard.sql                # ClickHouse query examples
└── .github/
    └── workflows/
        ├── test.yml                 # CI testing
        └── release.yml              # Release automation
```

### Design Principles

1. **Simplicity**: Minimal dependencies, clear separation of concerns
2. **Performance**: Batching, connection pooling, async operations
3. **Reliability**: Graceful error handling, structured logging
4. **Compatibility**: Follow k6 output extension conventions
5. **Production-ready**: Comprehensive testing, monitoring, documentation

## 2. Core Components

### 2.1 Extension Registration (`register.go`)

**Purpose**: Register the extension with k6's output system.

```go
package clickhouse

import (
    "github.com/mkutlak/xk6-output-clickhouse/pkg/clickhouse"
    "go.k6.io/k6/output"
)

func init() {
    output.RegisterExtension("clickhouse", func(params output.Params) (output.Output, error) {
        return clickhouse.New(params)
    })
}
```

**Key Points**:
- Uses Go's `init()` for automatic registration
- Extension name: `"clickhouse"`
- Factory function delegates to `pkg/clickhouse`
- Enables usage: `k6 run --out clickhouse script.js`

### 2.2 Configuration System (`pkg/clickhouse/config.go`)

**Purpose**: Manage configuration from multiple sources with proper precedence.

#### Configuration Structure

```go
package clickhouse

import (
    "encoding/json"
    "fmt"
    "time"

    "gopkg.in/guregu/null.v4"
    "go.k6.io/k6/lib/types"
    "go.k6.io/k6/output"
)

// Config holds the configuration for the ClickHouse output
type Config struct {
    // Connection settings
    Addr              null.String        `json:"addr" envconfig:"K6_CLICKHOUSE_ADDR"`
    Database          null.String        `json:"database" envconfig:"K6_CLICKHOUSE_DATABASE"`
    User              null.String        `json:"user" envconfig:"K6_CLICKHOUSE_USER"`
    Password          null.String        `json:"password" envconfig:"K6_CLICKHOUSE_PASSWORD"`

    // Table settings
    SamplesTable      null.String        `json:"samplesTable" envconfig:"K6_CLICKHOUSE_SAMPLES_TABLE"`

    // Performance tuning
    PushInterval      types.NullDuration `json:"pushInterval" envconfig:"K6_CLICKHOUSE_PUSH_INTERVAL"`
    MaxBatchSize      null.Int           `json:"maxBatchSize" envconfig:"K6_CLICKHOUSE_MAX_BATCH_SIZE"`
    AsyncInsert       null.Bool          `json:"asyncInsert" envconfig:"K6_CLICKHOUSE_ASYNC_INSERT"`

    // Connection pool settings
    MaxOpenConns      null.Int           `json:"maxOpenConns" envconfig:"K6_CLICKHOUSE_MAX_OPEN_CONNS"`
    MaxIdleConns      null.Int           `json:"maxIdleConns" envconfig:"K6_CLICKHOUSE_MAX_IDLE_CONNS"`
    ConnMaxLifetime   types.NullDuration `json:"connMaxLifetime" envconfig:"K6_CLICKHOUSE_CONN_MAX_LIFETIME"`

    // Schema management
    CreateDatabase    null.Bool          `json:"createDatabase" envconfig:"K6_CLICKHOUSE_CREATE_DATABASE"`
    CreateTables      null.Bool          `json:"createTables" envconfig:"K6_CLICKHOUSE_CREATE_TABLES"`

    // TLS settings
    TLS               null.Bool          `json:"tls" envconfig:"K6_CLICKHOUSE_TLS"`
    InsecureSkipVerify null.Bool         `json:"insecureSkipVerify" envconfig:"K6_CLICKHOUSE_INSECURE_SKIP_VERIFY"`

    // Tag handling
    TagsAsColumns     []string           `json:"tagsAsColumns" envconfig:"K6_CLICKHOUSE_TAGS_AS_COLUMNS"`
}

// NewConfig creates a new Config with default values
func NewConfig() Config {
    return Config{
        Addr:            null.StringFrom("localhost:9000"),
        Database:        null.StringFrom("k6"),
        User:            null.StringFrom("default"),
        Password:        null.StringFrom(""),
        SamplesTable:    null.StringFrom("samples"),
        PushInterval:    types.NullDurationFrom(1 * time.Second),
        MaxBatchSize:    null.IntFrom(1000),
        AsyncInsert:     null.BoolFrom(true),
        MaxOpenConns:    null.IntFrom(10),
        MaxIdleConns:    null.IntFrom(5),
        ConnMaxLifetime: types.NullDurationFrom(1 * time.Hour),
        CreateDatabase:  null.BoolFrom(false),
        CreateTables:    null.BoolFrom(true),
        TLS:             null.BoolFrom(false),
        InsecureSkipVerify: null.BoolFrom(false),
        TagsAsColumns:   []string{},
    }
}
```

#### Configuration Precedence

**Priority (lowest to highest)**:
1. Default values (hardcoded in `NewConfig()`)
2. JSON configuration (from k6 script or config file)
3. Environment variables (prefixed with `K6_CLICKHOUSE_`)
4. URL parameters (from `--out clickhouse=url?param=value`)

#### Configuration Consolidation

```go
func GetConsolidatedConfig(params output.Params) (Config, error) {
    // Start with defaults
    config := NewConfig()

    // Apply JSON config if provided
    if params.JSONConfig != nil {
        jsonConf := Config{}
        if err := json.Unmarshal(params.JSONConfig, &jsonConf); err != nil {
            return config, fmt.Errorf("failed to parse JSON config: %w", err)
        }
        config = config.Apply(jsonConf)
    }

    // Apply environment variables
    envConf := Config{}
    if err := envconfig.Process("", &envConf, params.Environment); err != nil {
        return config, fmt.Errorf("failed to process environment variables: %w", err)
    }
    config = config.Apply(envConf)

    // Apply URL parameters (ConfigArgument)
    if params.ConfigArgument != "" {
        urlConf, err := ParseURL(params.ConfigArgument)
        if err != nil {
            return config, fmt.Errorf("failed to parse URL config: %w", err)
        }
        config = config.Apply(urlConf)
    }

    // Validate configuration
    if err := config.Validate(); err != nil {
        return config, fmt.Errorf("invalid configuration: %w", err)
    }

    return config, nil
}

// Apply merges another config, overwriting only set values
func (c Config) Apply(other Config) Config {
    if other.Addr.Valid {
        c.Addr = other.Addr
    }
    if other.Database.Valid {
        c.Database = other.Database
    }
    // ... repeat for all fields
    return c
}

// Validate checks that the configuration is valid
func (c Config) Validate() error {
    if c.Addr.String == "" {
        return fmt.Errorf("addr is required")
    }
    if c.Database.String == "" {
        return fmt.Errorf("database is required")
    }
    if c.SamplesTable.String == "" {
        return fmt.Errorf("samplesTable is required")
    }
    if c.MaxBatchSize.Int64 <= 0 {
        return fmt.Errorf("maxBatchSize must be positive")
    }
    if c.PushInterval.Duration <= 0 {
        return fmt.Errorf("pushInterval must be positive")
    }
    return nil
}
```

#### Usage Examples

**Via JSON config:**
```javascript
export let options = {
    ext: {
        clickhouse: {
            addr: "clickhouse.example.com:9000",
            database: "k6_metrics",
            user: "k6user",
            password: "secret",
            pushInterval: "5s",
            maxBatchSize: 5000
        }
    }
};
```

**Via environment variables:**
```bash
export K6_CLICKHOUSE_ADDR="clickhouse.example.com:9000"
export K6_CLICKHOUSE_DATABASE="k6_metrics"
export K6_CLICKHOUSE_USER="k6user"
export K6_CLICKHOUSE_PASSWORD="secret"
k6 run --out clickhouse script.js
```

**Via URL parameters:**
```bash
k6 run --out clickhouse=clickhouse.example.com:9000?database=k6_metrics&user=k6user script.js
```

### 2.3 Output Implementation (`pkg/clickhouse/output.go`)

**Purpose**: Core logic for collecting, batching, and writing k6 metrics to ClickHouse.

#### Output Structure

```go
package clickhouse

import (
    "context"
    "database/sql"
    "fmt"
    "sync"
    "time"

    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/sirupsen/logrus"
    "go.k6.io/k6/metrics"
    "go.k6.io/k6/output"
)

// Output implements the output.Output interface for ClickHouse
type Output struct {
    output.SampleBuffer  // Embedded interface for automatic buffering

    config          Config
    logger          *logrus.Entry
    params          output.Params

    // Database connection
    conn            *sql.DB

    // Flushing
    periodicFlusher *output.PeriodicFlusher

    // Concurrency control
    wg              sync.WaitGroup

    // Schema management
    schemaManager   *SchemaManager
}

// New creates a new ClickHouse output
func New(params output.Params) (*Output, error) {
    // Get consolidated configuration
    config, err := GetConsolidatedConfig(params)
    if err != nil {
        return nil, fmt.Errorf("failed to get config: %w", err)
    }

    // Create logger
    logger := params.Logger.WithFields(logrus.Fields{
        "output": "clickhouse",
        "addr":   config.Addr.String,
        "db":     config.Database.String,
    })

    logger.Debug("Creating ClickHouse output")

    return &Output{
        config: config,
        logger: logger,
        params: params,
    }, nil
}

// Description returns a human-readable description
func (o *Output) Description() string {
    return fmt.Sprintf("clickhouse (%s)", o.config.Addr.String)
}
```

#### Lifecycle Methods

```go
// Start initializes the connection and starts the periodic flusher
func (o *Output) Start() error {
    o.logger.Debug("Starting ClickHouse output")

    // Create database connection
    conn, err := o.createConnection()
    if err != nil {
        return fmt.Errorf("failed to create connection: %w", err)
    }
    o.conn = conn

    // Test connection
    if err := o.conn.Ping(); err != nil {
        return fmt.Errorf("failed to ping ClickHouse: %w", err)
    }

    o.logger.Info("Connected to ClickHouse")

    // Create schema manager
    o.schemaManager = NewSchemaManager(o.conn, o.config, o.logger)

    // Setup database and tables if needed
    if o.config.CreateDatabase.Bool {
        if err := o.schemaManager.CreateDatabase(); err != nil {
            o.logger.WithError(err).Warn("Failed to create database")
        }
    }

    if o.config.CreateTables.Bool {
        if err := o.schemaManager.CreateTables(); err != nil {
            return fmt.Errorf("failed to create tables: %w", err)
        }
    }

    // Start periodic flusher
    pf, err := output.NewPeriodicFlusher(
        time.Duration(o.config.PushInterval.Duration),
        o.flushMetrics,
    )
    if err != nil {
        return fmt.Errorf("failed to create periodic flusher: %w", err)
    }
    o.periodicFlusher = pf

    o.logger.WithField("interval", o.config.PushInterval.Duration).Info("Periodic flusher started")

    return nil
}

// Stop flushes remaining metrics and closes the connection
func (o *Output) Stop() error {
    o.logger.Debug("Stopping ClickHouse output")

    // Stop periodic flusher
    if o.periodicFlusher != nil {
        o.periodicFlusher.Stop()
    }

    // Wait for in-flight writes
    o.wg.Wait()

    // Close database connection
    if o.conn != nil {
        if err := o.conn.Close(); err != nil {
            o.logger.WithError(err).Warn("Error closing database connection")
        }
    }

    o.logger.Info("ClickHouse output stopped")
    return nil
}
```

#### Connection Management

```go
func (o *Output) createConnection() (*sql.DB, error) {
    // Build connection options
    options := &clickhouse.Options{
        Addr: []string{o.config.Addr.String},
        Auth: clickhouse.Auth{
            Database: o.config.Database.String,
            Username: o.config.User.String,
            Password: o.config.Password.String,
        },
        Settings: clickhouse.Settings{
            "max_execution_time": 60,
        },
        DialTimeout: 5 * time.Second,
        Compression: &clickhouse.Compression{
            Method: clickhouse.CompressionLZ4,
        },
    }

    // Configure TLS if enabled
    if o.config.TLS.Bool {
        options.TLS = &tls.Config{
            InsecureSkipVerify: o.config.InsecureSkipVerify.Bool,
        }
    }

    // Enable async inserts if configured
    if o.config.AsyncInsert.Bool {
        options.Settings["async_insert"] = 1
        options.Settings["wait_for_async_insert"] = 0
    }

    // Open connection
    conn := clickhouse.OpenDB(options)

    // Configure connection pool
    conn.SetMaxOpenConns(int(o.config.MaxOpenConns.Int64))
    conn.SetMaxIdleConns(int(o.config.MaxIdleConns.Int64))
    conn.SetConnMaxLifetime(time.Duration(o.config.ConnMaxLifetime.Duration))

    return conn, nil
}
```

#### Metric Flushing

```go
// flushMetrics is called periodically to write buffered samples
func (o *Output) flushMetrics() {
    start := time.Now()

    // Get buffered samples
    samples := o.GetBufferedSamples()
    if len(samples) == 0 {
        return
    }

    // Count total samples
    sampleCount := 0
    for _, container := range samples {
        sampleCount += len(container.GetSamples())
    }

    o.logger.WithField("samples", sampleCount).Debug("Flushing metrics")

    // Process in batches
    if err := o.processSamples(samples); err != nil {
        o.logger.WithError(err).Error("Failed to flush metrics")
        return
    }

    elapsed := time.Since(start)
    o.logger.WithFields(logrus.Fields{
        "samples": sampleCount,
        "elapsed": elapsed,
    }).Debug("Metrics flushed successfully")

    // Warn if flush took longer than push interval
    if elapsed > time.Duration(o.config.PushInterval.Duration) {
        o.logger.WithField("elapsed", elapsed).Warn(
            "Flush took longer than push interval. Consider increasing batch size or push interval.",
        )
    }
}

// processSamples converts and writes samples in batches
func (o *Output) processSamples(samples []metrics.SampleContainer) error {
    batch := make([]Sample, 0, o.config.MaxBatchSize.Int64)

    for _, container := range samples {
        for _, sample := range container.GetSamples() {
            // Convert k6 sample to ClickHouse sample
            chSample := o.convertSample(sample)
            batch = append(batch, chSample)

            // Write batch if full
            if len(batch) >= int(o.config.MaxBatchSize.Int64) {
                if err := o.writeBatch(batch); err != nil {
                    return err
                }
                batch = batch[:0] // Reset
            }
        }
    }

    // Write remaining samples
    if len(batch) > 0 {
        if err := o.writeBatch(batch); err != nil {
            return err
        }
    }

    return nil
}
```

#### Sample Conversion and Batch Writing

```go
// Sample represents a ClickHouse sample row
type Sample struct {
    Timestamp   time.Time
    MetricName  string
    MetricValue float64
    MetricType  string
    Tags        map[string]string
}

// convertSample converts a k6 sample to a ClickHouse sample
func (o *Output) convertSample(sample metrics.Sample) Sample {
    chSample := Sample{
        Timestamp:   sample.Time,
        MetricName:  sample.Metric.Name,
        MetricValue: sample.Value,
        MetricType:  sample.Metric.Type.String(),
        Tags:        make(map[string]string),
    }

    // Convert tags
    if sample.Tags != nil {
        for k, v := range sample.Tags.Map() {
            chSample.Tags[k] = v
        }
    }

    return chSample
}

// writeBatch writes a batch of samples to ClickHouse
func (o *Output) writeBatch(samples []Sample) error {
    if len(samples) == 0 {
        return nil
    }

    ctx := context.Background()

    // Begin batch transaction
    batch, err := o.conn.Begin()
    if err != nil {
        return fmt.Errorf("failed to begin batch: %w", err)
    }
    defer batch.Rollback()

    // Prepare insert statement
    stmt, err := batch.Prepare(fmt.Sprintf(`
        INSERT INTO %s.%s (
            timestamp,
            metric_name,
            metric_value,
            metric_type,
            tags
        ) VALUES (?, ?, ?, ?, ?)
    `, o.config.Database.String, o.config.SamplesTable.String))
    if err != nil {
        return fmt.Errorf("failed to prepare statement: %w", err)
    }
    defer stmt.Close()

    // Insert all samples
    for _, sample := range samples {
        _, err := stmt.Exec(
            sample.Timestamp,
            sample.MetricName,
            sample.MetricValue,
            sample.MetricType,
            sample.Tags,
        )
        if err != nil {
            return fmt.Errorf("failed to insert sample: %w", err)
        }
    }

    // Commit batch
    if err := batch.Commit(); err != nil {
        return fmt.Errorf("failed to commit batch: %w", err)
    }

    return nil
}
```

### 2.4 Schema Management (`pkg/clickhouse/schema.go`)

**Purpose**: Handle database and table creation with ClickHouse-optimized schemas.

```go
package clickhouse

import (
    "database/sql"
    "fmt"

    "github.com/sirupsen/logrus"
)

// SchemaManager handles database schema operations
type SchemaManager struct {
    conn   *sql.DB
    config Config
    logger *logrus.Entry
}

// NewSchemaManager creates a new schema manager
func NewSchemaManager(conn *sql.DB, config Config, logger *logrus.Entry) *SchemaManager {
    return &SchemaManager{
        conn:   conn,
        config: config,
        logger: logger,
    }
}

// CreateDatabase creates the database if it doesn't exist
func (sm *SchemaManager) CreateDatabase() error {
    query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sm.config.Database.String)

    sm.logger.WithField("query", query).Debug("Creating database")

    if _, err := sm.conn.Exec(query); err != nil {
        return fmt.Errorf("failed to create database: %w", err)
    }

    sm.logger.Info("Database created or already exists")
    return nil
}

// CreateTables creates the samples table with optimal schema
func (sm *SchemaManager) CreateTables() error {
    // Samples table with MergeTree engine
    samplesQuery := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s.%s (
            timestamp DateTime64(3),
            metric_name LowCardinality(String),
            metric_value Float64,
            metric_type LowCardinality(String),
            tags Map(String, String)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (metric_name, timestamp)
        TTL timestamp + INTERVAL 30 DAY
        SETTINGS index_granularity = 8192
    `, sm.config.Database.String, sm.config.SamplesTable.String)

    sm.logger.WithField("query", samplesQuery).Debug("Creating samples table")

    if _, err := sm.conn.Exec(samplesQuery); err != nil {
        return fmt.Errorf("failed to create samples table: %w", err)
    }

    sm.logger.Info("Samples table created or already exists")
    return nil
}
```

## 3. Database Schema Design

### 3.1 Samples Table

**Design considerations**:
- **MergeTree engine**: Optimized for time-series data with fast inserts
- **Partitioning**: Daily partitions for efficient data management and TTL
- **Ordering**: By metric_name and timestamp for fast queries
- **Data types**:
  - `DateTime64(3)`: Millisecond precision timestamps
  - `LowCardinality(String)`: Memory-efficient for repeated values (metric names, types)
  - `Map(String, String)`: Flexible tag storage
  - `Float64`: Metric values
- **TTL**: Automatic data expiration after 30 days (configurable)

```sql
CREATE TABLE k6.samples (
    timestamp DateTime64(3),
    metric_name LowCardinality(String),
    metric_value Float64,
    metric_type LowCardinality(String),
    tags Map(String, String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (metric_name, timestamp)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
```

### 3.2 Alternative Schema: Materialized Columns for Common Tags

For frequently queried tags, consider extracting them as materialized columns:

```sql
CREATE TABLE k6.samples (
    timestamp DateTime64(3),
    metric_name LowCardinality(String),
    metric_value Float64,
    metric_type LowCardinality(String),
    tags Map(String, String),

    -- Materialized columns for common tags
    test_run_id String MATERIALIZED tags['test_run_id'],
    scenario String MATERIALIZED tags['scenario'],
    url String MATERIALIZED tags['url'],
    method String MATERIALIZED tags['method'],
    status UInt16 MATERIALIZED toUInt16OrZero(tags['status'])
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (metric_name, test_run_id, timestamp)
SETTINGS index_granularity = 8192;
```

### 3.3 Buffer Table (Optional Performance Optimization)

For extreme write performance, use a Buffer table:

```sql
CREATE TABLE k6.samples_buffer AS k6.samples
ENGINE = Buffer(
    k6,              -- destination database
    samples,         -- destination table
    16,              -- num_layers
    10,              -- min_time (seconds)
    100,             -- max_time (seconds)
    10000,           -- min_rows
    1000000,         -- max_rows
    10000000,        -- min_bytes
    100000000        -- max_bytes
);
```

## 4. Implementation Phases

### Phase 1: MVP (Minimum Viable Product)

**Goal**: Basic functionality with core features.

**Tasks**:
1. ✅ Project structure setup
   - Create directory structure
   - Initialize Go module
   - Setup Makefile

2. ✅ Configuration system
   - Implement `Config` struct with nullable types
   - Environment variable parsing
   - JSON config parsing
   - Configuration validation

3. ✅ Basic output implementation
   - Extension registration
   - `New()` constructor
   - `Description()` method
   - Connection management

4. ✅ Database operations
   - ClickHouse connection with `clickhouse-go/v2`
   - Schema creation (database + samples table)
   - Basic INSERT implementation

5. ✅ Metric collection
   - Embed `output.SampleBuffer`
   - Implement `Start()` and `Stop()`
   - Setup `PeriodicFlusher`
   - Sample conversion
   - Batch writing

6. ✅ Testing
   - Unit tests for configuration
   - Integration tests with ClickHouse testcontainer
   - Example k6 scripts

**Deliverables**:
- Working extension that can write k6 metrics to ClickHouse
- Basic documentation
- Example scripts

### Phase 2: Performance Optimization

**Goal**: Production-ready performance and reliability.

**Tasks**:
1. ✅ Batching optimization
   - Configurable batch size
   - Smart batch sizing based on throughput
   - Performance benchmarking

2. ✅ Async operations
   - Async insert support
   - WaitGroup for graceful shutdown
   - Error handling for failed writes

3. ✅ Connection pooling
   - Optimize pool settings
   - Connection health checks
   - Reconnection logic

4. ✅ Monitoring and observability
   - Performance metrics logging
   - Flush timing warnings
   - Batch size monitoring
   - Error rate tracking

5. ✅ Advanced schema options
   - Materialized columns for common tags
   - Optional Buffer table
   - Configurable TTL

**Deliverables**:
- Optimized performance (>10k samples/sec)
- Production-ready error handling
- Performance benchmarks

### Phase 3: Advanced Features

**Goal**: Enterprise features and ecosystem integration.

**Tasks**:
1. ✅ Advanced configuration
   - TLS support
   - Multiple ClickHouse nodes (cluster support)
   - Authentication methods
   - Custom table schemas

2. ✅ Query helpers
   - Grafana datasource examples
   - Common query templates
   - Dashboard examples

3. ✅ Data retention
   - Configurable TTL
   - Data archival strategies
   - Aggregated tables for long-term storage

4. ✅ Documentation
   - Comprehensive README
   - Architecture documentation
   - Performance tuning guide
   - Grafana integration guide

5. ✅ CI/CD
   - GitHub Actions workflows
   - Automated testing
   - Release automation
   - Docker image publishing

**Deliverables**:
- Feature-complete extension
- Comprehensive documentation
- Grafana dashboards
- Automated releases

## 5. Dependencies

### Core Dependencies

```go
// go.mod
module github.com/mkutlak/xk6-output-clickhouse

go 1.21

require (
    github.com/ClickHouse/clickhouse-go/v2 v2.15.0
    github.com/kelseyhightower/envconfig v1.4.0
    github.com/sirupsen/logrus v1.9.3
    go.k6.io/k6 v0.48.0
    gopkg.in/guregu/null.v4 v4.0.0
)
```

### Development Dependencies

```go
require (
    github.com/stretchr/testify v1.8.4
    github.com/testcontainers/testcontainers-go v0.26.0
)
```

### Dependency Rationale

- **clickhouse-go/v2**: Official ClickHouse driver with connection pooling, compression, and async support
- **envconfig**: Environment variable parsing with struct tags
- **logrus**: Structured logging (same as other k6 extensions)
- **k6**: Core k6 libraries for output interface
- **null.v4**: Nullable types for configuration
- **testify**: Testing assertions
- **testcontainers**: Integration testing with real ClickHouse

## 6. Testing Strategy

### 6.1 Unit Tests

**Coverage targets**:
- Configuration parsing and validation: 100%
- Sample conversion: 100%
- Error handling: >90%

**Test files**:
- `config_test.go`: Configuration merging, validation, parsing
- `output_test.go`: Sample conversion, batching logic

**Example**:
```go
func TestConfigValidation(t *testing.T) {
    tests := []struct {
        name    string
        config  Config
        wantErr bool
    }{
        {
            name: "valid config",
            config: Config{
                Addr:         null.StringFrom("localhost:9000"),
                Database:     null.StringFrom("k6"),
                SamplesTable: null.StringFrom("samples"),
                MaxBatchSize: null.IntFrom(1000),
                PushInterval: types.NullDurationFrom(1 * time.Second),
            },
            wantErr: false,
        },
        {
            name: "missing addr",
            config: Config{
                Database: null.StringFrom("k6"),
            },
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := tt.config.Validate()
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}
```

### 6.2 Integration Tests

**Test with testcontainers**:
```go
func TestIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }

    // Start ClickHouse container
    ctx := context.Background()
    req := testcontainers.ContainerRequest{
        Image:        "clickhouse/clickhouse-server:latest",
        ExposedPorts: []string{"9000/tcp", "8123/tcp"},
    }

    container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: req,
        Started:          true,
    })
    require.NoError(t, err)
    defer container.Terminate(ctx)

    // Get connection details
    host, err := container.Host(ctx)
    require.NoError(t, err)
    port, err := container.MappedPort(ctx, "9000")
    require.NoError(t, err)

    // Test output
    params := output.Params{
        ConfigArgument: fmt.Sprintf("%s:%s", host, port.Port()),
        Logger:         logrus.NewEntry(logrus.New()),
    }

    out, err := New(params)
    require.NoError(t, err)

    err = out.Start()
    require.NoError(t, err)

    // ... test metric writing ...

    err = out.Stop()
    require.NoError(t, err)
}
```

### 6.3 Performance Tests

**Benchmark targets**:
- 10,000+ samples/second write throughput
- <100ms p99 flush latency
- Minimal memory overhead

**Benchmark example**:
```go
func BenchmarkWriteBatch(b *testing.B) {
    // Setup
    out := setupTestOutput(b)
    samples := generateSamples(1000)

    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            out.writeBatch(samples)
        }
    })
}
```

### 6.4 Example Scripts

**Simple example** (`examples/simple.js`):
```javascript
import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
    vus: 10,
    duration: '30s',
    ext: {
        clickhouse: {
            addr: 'localhost:9000',
            database: 'k6',
        }
    }
};

export default function() {
    http.get('https://test.k6.io');
    sleep(1);
}
```

**Advanced example** (`examples/advanced.js`):
```javascript
import http from 'k6/http';
import { check } from 'k6';

export const options = {
    stages: [
        { duration: '1m', target: 50 },
        { duration: '3m', target: 50 },
        { duration: '1m', target: 0 },
    ],
    thresholds: {
        http_req_duration: ['p(95)<500'],
    },
    ext: {
        clickhouse: {
            addr: 'clickhouse.example.com:9000',
            database: 'k6_prod',
            user: 'k6user',
            password: 'secret',
            tls: true,
            pushInterval: '5s',
            maxBatchSize: 5000,
            asyncInsert: true,
        }
    }
};

export default function() {
    const res = http.get('https://api.example.com/users');
    check(res, {
        'status is 200': (r) => r.status === 200,
        'response time < 500ms': (r) => r.timings.duration < 500,
    });
}
```

## 7. Documentation Plan

### 7.1 README.md

**Sections**:
1. Introduction and overview
2. Installation via xk6
3. Quick start guide
4. Configuration reference
5. Usage examples
6. ClickHouse schema details
7. Querying metrics
8. Grafana integration
9. Performance tuning
10. Troubleshooting
11. Contributing guidelines

### 7.2 Architecture Documentation

**File**: `ARCHITECTURE.md`

**Content**:
- High-level architecture diagram
- Data flow explanation
- Component interactions
- Design decisions and rationale
- Performance characteristics

### 7.3 Performance Guide

**File**: `PERFORMANCE.md`

**Content**:
- Recommended configuration for different scales
- Batch size tuning
- Connection pool sizing
- ClickHouse server tuning
- Benchmarking methodology
- Troubleshooting slow writes

### 7.4 Grafana Integration Guide

**File**: `GRAFANA.md`

**Content**:
- ClickHouse datasource setup
- Example dashboards (JSON)
- Common queries for metrics visualization
- Alert configuration examples

## 8. Performance Targets

### 8.1 Write Performance

**Targets**:
- **Throughput**: >10,000 samples/second
- **Latency**: p99 < 100ms per flush
- **CPU usage**: <10% for k6 extension overhead
- **Memory usage**: <100MB for buffering and batching

### 8.2 Optimization Strategies

1. **Batching**
   - Default batch size: 1000 rows
   - Auto-adjust based on throughput
   - Flush on interval OR batch size

2. **Async inserts**
   - Enable ClickHouse async_insert
   - Reduces write latency
   - Acceptable for observability data

3. **Compression**
   - Enable LZ4 compression on connection
   - Reduces network bandwidth
   - Minimal CPU overhead

4. **Connection pooling**
   - 10 max connections (default)
   - 5 idle connections
   - 1-hour max lifetime

5. **Buffer table** (optional)
   - For extreme throughput (>50k samples/sec)
   - Trades durability for performance
   - Configure flush thresholds appropriately

## 9. Success Criteria

### 9.1 Functional Requirements

- ✅ Writes all k6 metric types (counter, gauge, rate, trend)
- ✅ Preserves metric metadata (name, type, timestamp, tags)
- ✅ Supports multiple configuration sources
- ✅ Creates database and tables automatically
- ✅ Handles connection failures gracefully
- ✅ Flushes metrics periodically and on shutdown

### 9.2 Non-Functional Requirements

- ✅ Minimal overhead (<5% of k6 execution time)
- ✅ No data loss during normal operation
- ✅ Clear error messages for troubleshooting
- ✅ Production-ready logging
- ✅ Comprehensive documentation

### 9.3 Quality Gates

- ✅ >80% code coverage
- ✅ All tests passing (unit + integration)
- ✅ No critical security vulnerabilities
- ✅ Performance benchmarks meet targets
- ✅ Documentation complete and accurate

## 10. Release Plan

### 10.1 Versioning

Follow Semantic Versioning (semver):
- **v0.1.0**: MVP release
- **v0.2.0**: Performance optimizations
- **v1.0.0**: Production-ready with all features

### 10.2 Release Checklist

- [ ] All tests passing
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Version bumped in code
- [ ] Git tag created
- [ ] GitHub release created
- [ ] Announcement in k6 community

### 10.3 Distribution

**Build with xk6**:
```bash
xk6 build --with github.com/mkutlak/xk6-output-clickhouse@latest
```

**Docker image**:
```dockerfile
FROM grafana/xk6:latest AS builder
RUN xk6 build --with github.com/mkutlak/xk6-output-clickhouse@latest

FROM alpine:latest
COPY --from=builder /root/k6 /usr/bin/k6
ENTRYPOINT ["/usr/bin/k6"]
```

## 11. Risk Mitigation

### 11.1 Technical Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| ClickHouse connection failures | High | Medium | Retry logic, connection pooling, clear error messages |
| Write performance bottleneck | High | Low | Batching, async inserts, benchmarking early |
| Data loss on crash | Medium | Low | Graceful shutdown, flush on Stop() |
| Memory leaks | Medium | Low | Proper cleanup, connection pool limits |
| Breaking changes in k6 API | Low | Low | Pin k6 version, monitor releases |

### 11.2 Operational Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Insufficient documentation | High | Medium | Comprehensive docs, examples, guides |
| User misconfiguration | Medium | High | Sensible defaults, validation, error messages |
| ClickHouse version compatibility | Low | Medium | Test with multiple versions, document requirements |

## 12. Future Enhancements

**Post-v1.0 features** (prioritized):

1. **Distributed tracing support**
   - Trace ID correlation
   - Span storage

2. **Custom aggregations**
   - Pre-aggregate common queries
   - Materialized views for dashboards

3. **Multi-region support**
   - Write to multiple ClickHouse clusters
   - Automatic failover

4. **Schema evolution**
   - Automatic schema migrations
   - Backward compatibility

5. **Advanced tag handling**
   - Tag filtering/allowlist
   - Tag cardinality limits
   - Automatic tag extraction

6. **Metrics streaming**
   - Real-time metric streaming API
   - WebSocket support for live dashboards

## 13. Appendix

### 13.1 Comparison with Other Outputs

| Feature | InfluxDB | TimescaleDB | ClickHouse |
|---------|----------|-------------|------------|
| Protocol | HTTP | PostgreSQL (COPY) | Native TCP |
| Batching | 1s interval | 1000 rows | 1000 rows |
| Compression | Yes | No | Yes (LZ4) |
| Async writes | No | No | Yes |
| Tags storage | Native | JSONB | Map type |
| Performance | Good | Excellent | Excellent |
| Query language | InfluxQL/Flux | SQL | SQL |
| Clustering | Enterprise | Built-in | Built-in |

### 13.2 ClickHouse Query Examples

**Get metrics for a specific test run**:
```sql
SELECT
    metric_name,
    avg(metric_value) as avg_value,
    quantile(0.95)(metric_value) as p95_value
FROM k6.samples
WHERE tags['test_run_id'] = 'abc123'
    AND timestamp >= now() - INTERVAL 1 HOUR
GROUP BY metric_name
ORDER BY metric_name;
```

**HTTP request duration percentiles**:
```sql
SELECT
    toStartOfMinute(timestamp) as time,
    quantile(0.50)(metric_value) as p50,
    quantile(0.95)(metric_value) as p95,
    quantile(0.99)(metric_value) as p99
FROM k6.samples
WHERE metric_name = 'http_req_duration'
    AND timestamp >= now() - INTERVAL 1 HOUR
GROUP BY time
ORDER BY time;
```

**Error rate by URL**:
```sql
SELECT
    tags['url'] as url,
    countIf(metric_value > 0) as errors,
    count() as total,
    (errors / total) * 100 as error_rate_pct
FROM k6.samples
WHERE metric_name = 'http_req_failed'
    AND timestamp >= now() - INTERVAL 1 HOUR
GROUP BY url
ORDER BY error_rate_pct DESC
LIMIT 10;
```

### 13.3 Grafana Dashboard Example

**Panel query for request rate**:
```sql
SELECT
    $__timeInterval(timestamp) as time,
    sum(metric_value) / $__interval_s as requests_per_sec
FROM $database.$table
WHERE $__timeFilter(timestamp)
    AND metric_name = 'http_reqs'
GROUP BY time
ORDER BY time
```

---

## Summary

This implementation plan provides a comprehensive roadmap for building **xk6-output-clickhouse**, a production-ready k6 extension for streaming performance metrics directly to ClickHouse. The design follows proven patterns from existing Grafana extensions while leveraging ClickHouse's unique strengths for time-series data.

**Key takeaways**:
1. **Proven architecture**: Based on successful InfluxDB, TimescaleDB, and Timestream implementations
2. **Performance-first**: Batching, compression, async inserts, connection pooling
3. **Production-ready**: Comprehensive error handling, logging, testing, documentation
4. **ClickHouse-optimized**: MergeTree engine, partitioning, LowCardinality, Map types
5. **Developer-friendly**: Clear configuration, sensible defaults, extensive examples

**Next steps**:
1. Create project repository structure
2. Implement Phase 1 (MVP)
3. Test with real k6 workloads
4. Iterate based on feedback
5. Release v1.0.0
