package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.k6.io/k6/output"
	"go.uber.org/zap"
)

// Output implements the output.Output interface
type Output struct {
	output.SampleBuffer
	config          Config
	logger          *zap.Logger
	db              *sql.DB
	periodicFlusher *output.PeriodicFlusher
}

// New creates a new ClickHouse output
func New(params output.Params) (output.Output, error) {
	cfg, err := ParseConfig(params)
	if err != nil {
		return nil, err
	}

	// Convert logrus logger to zap logger
	// k6 uses logrus, so we need to create a new zap logger
	logger, _ := zap.NewProduction()

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
	o.logger.Debug("Stopping")

	if o.periodicFlusher != nil {
		o.periodicFlusher.Stop()
	}

	if o.db != nil {
		o.db.Close()
	}

	o.logger.Info("Stopped")
	return nil
}

// flush writes buffered samples to ClickHouse
func (o *Output) flush() {
	samples := o.GetBufferedSamples()
	if len(samples) == 0 {
		return
	}

	start := time.Now()
	ctx := context.Background()

	// Prepare batch insert
	batch, err := o.db.Begin()
	if err != nil {
		o.logger.Error("Failed to begin batch", zap.Error(err))
		return
	}
	defer batch.Rollback()

	// Use appropriate INSERT query based on schema mode
	var insertQuery string
	if o.config.SchemaMode == "compatible" {
		insertQuery = fmt.Sprintf(`
			INSERT INTO %s.%s (
				timestamp, build_id, release, version, branch,
				metric_name, metric_type, value, testid,
				ui_feature, scenario, name, method, status,
				expected_response, error_code, rating, resource_type,
				check_name, group_name, extra_tags
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, o.config.Database, o.config.Table)
	} else {
		insertQuery = fmt.Sprintf(`
			INSERT INTO %s.%s (timestamp, metric_name, metric_value, tags) VALUES (?, ?, ?, ?)
		`, o.config.Database, o.config.Table)
	}

	stmt, err := batch.Prepare(insertQuery)
	if err != nil {
		o.logger.Error("Failed to prepare statement", zap.Error(err))
		return
	}
	defer stmt.Close()

	count := 0
	for _, container := range samples {
		for _, sample := range container.GetSamples() {
			var err error

			if o.config.SchemaMode == "compatible" {
				cs := ConvertToCompatible(sample)
				_, err = stmt.ExecContext(ctx,
					cs.Timestamp, cs.BuildID, cs.Release, cs.Version, cs.Branch,
					cs.MetricName, cs.MetricType, cs.Value, cs.TestID,
					cs.UIFeature, cs.Scenario, cs.Name, cs.Method, cs.Status,
					cs.ExpectedResponse, cs.ErrorCode, cs.Rating, cs.ResourceType,
					cs.CheckName, cs.GroupName, cs.ExtraTags,
				)
			} else {
				ss := ConvertToSimple(sample)
				_, err = stmt.ExecContext(ctx,
					ss.Timestamp,
					ss.MetricName,
					ss.MetricValue,
					ss.Tags,
				)
			}

			if err != nil {
				o.logger.Error("Failed to insert sample", zap.Error(err))
				continue
			}
			count++
		}
	}

	if err := batch.Commit(); err != nil {
		o.logger.Error("Failed to commit batch", zap.Error(err))
		return
	}

	o.logger.Debug("Flushed metrics",
		zap.Int("samples", count),
		zap.Duration("elapsed", time.Since(start)))
}
