package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

// Output implements the output.Output interface
type Output struct {
	output.SampleBuffer
	config          Config
	logger          *logrus.Entry
	db              *sql.DB
	periodicFlusher *output.PeriodicFlusher
}

// New creates a new ClickHouse output
func New(params output.Params) (output.Output, error) {
	cfg, err := ParseConfig(params)
	if err != nil {
		return nil, err
	}

	return &Output{
		config: cfg,
		logger: params.Logger.WithField("output", "clickhouse"),
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
			Username: "default",
			Password: "",
		},
	})

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	o.db = db
	o.logger.Info("Connected to ClickHouse")

	// Create schema
	if err := createSchema(db, o.config.Database, o.config.Table); err != nil {
		return err
	}

	o.logger.Info("Schema created")

	// Start periodic flusher
	pf, err := output.NewPeriodicFlusher(o.config.PushInterval, o.flush)
	if err != nil {
		return err
	}
	o.periodicFlusher = pf

	o.logger.WithField("interval", o.config.PushInterval).Info("Started")
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
		o.logger.WithError(err).Error("Failed to begin batch")
		return
	}
	defer batch.Rollback()

	stmt, err := batch.Prepare(fmt.Sprintf(`
		INSERT INTO %s.%s (timestamp, metric_name, metric_value, tags)
	`, o.config.Database, o.config.Table))
	if err != nil {
		o.logger.WithError(err).Error("Failed to prepare statement")
		return
	}
	defer stmt.Close()

	count := 0
	for _, container := range samples {
		for _, sample := range container.GetSamples() {
			tags := make(map[string]string)
			if sample.Tags != nil {
				for k, v := range sample.Tags.Map() {
					tags[k] = v
				}
			}

			_, err := stmt.ExecContext(ctx,
				sample.Time,
				sample.Metric.Name,
				sample.Value,
				tags,
			)
			if err != nil {
				o.logger.WithError(err).Error("Failed to insert sample")
				continue
			}
			count++
		}
	}

	if err := batch.Commit(); err != nil {
		o.logger.WithError(err).Error("Failed to commit batch")
		return
	}

	o.logger.WithFields(logrus.Fields{
		"samples": count,
		"elapsed": time.Since(start),
	}).Debug("Flushed metrics")
}
