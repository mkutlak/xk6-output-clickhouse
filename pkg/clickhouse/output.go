package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.k6.io/k6/output"
	"go.uber.org/zap"
)

// escapeIdentifier escapes a ClickHouse identifier with backticks
func escapeIdentifier(name string) string {
	return "`" + name + "`"
}

// Output implements the output.Output interface
type Output struct {
	output.SampleBuffer
	config          Config
	logger          *zap.Logger
	db              *sql.DB
	periodicFlusher *output.PeriodicFlusher
	insertQuery     string // Pre-computed INSERT query

	// Concurrency control
	mu     sync.RWMutex
	closed bool
}

// New creates a new ClickHouse output
func New(params output.Params) (output.Output, error) {
	cfg, err := ParseConfig(params)
	if err != nil {
		return nil, err
	}

	// Convert logrus logger to zap logger
	// k6 uses logrus, so we need to create a new zap logger
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return &Output{
		config: cfg,
		logger: logger.With(zap.String("output", "clickhouse")),
	}, nil
}

// Description returns a human-readable description
func (o *Output) Description() string {
	return fmt.Sprintf("clickhouse (%s)", o.config.Addr)
}

// Start initializes the connection and starts the flusher
func (o *Output) Start() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return fmt.Errorf("output already closed")
	}

	o.logger.Debug("Starting ClickHouse output")

	// Connect to ClickHouse
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{o.config.Addr},
		Auth: clickhouse.Auth{
			Database: o.config.Database,
			Username: o.config.User,
			Password: o.config.Password,
		},
	})

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	o.db = db
	o.logger.Info("Connected to ClickHouse")

	// Create schema only if not skipped and in simple mode
	if !o.config.SkipSchemaCreation {
		if o.config.SchemaMode == "simple" {
			if err := createSchema(db, o.config.Database, o.config.Table); err != nil {
				return err
			}
			o.logger.Info("Schema created")
		} else {
			o.logger.Warn("Compatible schema mode enabled with skipSchemaCreation=false. Skipping schema creation as compatible schema should already exist.")
		}
	} else {
		o.logger.Info("Schema creation skipped")
	}

	// Pre-compute INSERT query to avoid allocations on every flush
	if o.config.SchemaMode == "compatible" {
		o.insertQuery = fmt.Sprintf(`
			INSERT INTO %s.%s (
				timestamp, build_id, release, version, branch,
				metric_name, metric_type, value, testid,
				ui_feature, scenario, name, method, status,
				expected_response, error_code, rating, resource_type,
				check_name, group_name, extra_tags
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, escapeIdentifier(o.config.Database), escapeIdentifier(o.config.Table))
	} else {
		o.insertQuery = fmt.Sprintf(`
			INSERT INTO %s.%s (timestamp, metric_name, metric_value, tags) VALUES (?, ?, ?, ?)
		`, escapeIdentifier(o.config.Database), escapeIdentifier(o.config.Table))
	}

	// Start periodic flusher
	pf, err := output.NewPeriodicFlusher(o.config.PushInterval, o.flush)
	if err != nil {
		return err
	}
	o.periodicFlusher = pf

	o.logger.Info("Started", zap.Duration("interval", o.config.PushInterval))
	return nil
}

// Stop flushes remaining metrics and closes the connection
func (o *Output) Stop() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil // Already stopped
	}
	o.closed = true
	o.mu.Unlock()

	o.logger.Debug("Stopping")

	if o.periodicFlusher != nil {
		o.periodicFlusher.Stop()
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.db != nil {
		o.db.Close()
	}

	o.logger.Info("Stopped")
	return nil
}

// flush writes buffered samples to ClickHouse
func (o *Output) flush() {
	o.mu.RLock()
	if o.closed {
		o.mu.RUnlock()
		return
	}

	// Capture state under lock
	db := o.db
	insertQuery := o.insertQuery
	config := o.config
	logger := o.logger
	o.mu.RUnlock()

	samples := o.GetBufferedSamples()
	if len(samples) == 0 {
		return
	}

	start := time.Now()
	ctx := context.Background()

	// Prepare batch insert
	batch, err := db.Begin()
	if err != nil {
		logger.Error("Failed to begin batch", zap.Error(err))
		return
	}
	defer batch.Rollback()

	stmt, err := batch.Prepare(insertQuery)
	if err != nil {
		logger.Error("Failed to prepare statement", zap.Error(err))
		return
	}
	defer stmt.Close()

	count := 0
	for _, container := range samples {
		for _, sample := range container.GetSamples() {
			var err error

			if config.SchemaMode == "compatible" {
				cs := ConvertToCompatible(ctx, sample)
				_, err = stmt.ExecContext(ctx,
					cs.Timestamp, cs.BuildID, cs.Release, cs.Version, cs.Branch,
					cs.MetricName, cs.MetricType, cs.Value, cs.TestID,
					cs.UIFeature, cs.Scenario, cs.Name, cs.Method, cs.Status,
					cs.ExpectedResponse, cs.ErrorCode, cs.Rating, cs.ResourceType,
					cs.CheckName, cs.GroupName, cs.ExtraTags,
				)
			} else {
				ss := ConvertToSimple(ctx, sample)
				_, err = stmt.ExecContext(ctx,
					ss.Timestamp,
					ss.MetricName,
					ss.MetricValue,
					ss.Tags,
				)
			}

			if err != nil {
				logger.Error("Failed to insert sample", zap.Error(err))
				continue
			}
			count++
		}
	}

	if err := batch.Commit(); err != nil {
		logger.Error("Failed to commit batch", zap.Error(err))
		return
	}

	logger.Debug("Flushed metrics",
		zap.Int("samples", count),
		zap.Duration("elapsed", time.Since(start)))
}
