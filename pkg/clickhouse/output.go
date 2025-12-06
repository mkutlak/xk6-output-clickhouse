package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.k6.io/k6/output"
	"go.uber.org/zap"
)

// Memory pools for reducing allocations during high-throughput operations
var (
	// tagMapPool reuses map[string]string for tag storage
	// Maps are cleared before returning to pool to prevent memory leaks
	tagMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]string)
		},
	}

	// compatibleRowPool reuses []interface{} slices for compatible schema rows (21 fields)
	// Pre-sized to avoid slice growth during append operations
	compatibleRowPool = sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 21)
		},
	}

	// simpleRowPool reuses []interface{} slices for simple schema rows (4 fields)
	// Pre-sized to match simple schema field count
	simpleRowPool = sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 4)
		},
	}
)

// clearMap efficiently clears a map while retaining its allocated capacity
// This avoids map reallocations when the map is reused from the pool
func clearMap(m map[string]string) {
	for k := range m {
		delete(m, k)
	}
}

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
	mu      sync.RWMutex
	closed  bool
	flushWG sync.WaitGroup // Track in-flight flushes

	// Context cancellation for graceful shutdown
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
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

	// Create cancellable context for graceful shutdown
	o.shutdownCtx, o.shutdownCancel = context.WithCancel(context.Background())

	o.logger.Debug("Starting ClickHouse output")

	// Build TLS configuration
	tlsConfig, err := o.config.TLS.BuildTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to build TLS config: %w", err)
	}

	// Warn if using port 9000 with TLS (should use 9440)
	if o.config.TLS.Enabled && strings.Contains(o.config.Addr, ":9000") {
		o.logger.Warn("TLS is enabled but using port 9000. Consider using port 9440 for secure connections.")
	}

	// Log TLS status
	if o.config.TLS.Enabled {
		if o.config.TLS.InsecureSkipVerify {
			o.logger.Warn("TLS enabled with InsecureSkipVerify=true. Certificate verification is DISABLED. This is insecure and should only be used for testing.")
		} else {
			o.logger.Info("TLS enabled with certificate verification")
		}
	} else {
		o.logger.Debug("TLS disabled, using unencrypted connection")
	}

	// Connect to ClickHouse
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{o.config.Addr},
		Auth: clickhouse.Auth{
			Database: o.config.Database,
			Username: o.config.User,
			Password: o.config.Password,
		},
		TLS: tlsConfig,
	})

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping clickhouse: %w", err)
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
				metric, metric_type, value, testid,
				ui_feature, scenario, name, method, status,
				expected_response, error_code, rating, resource_type,
				check_name, group_name, extra_tags
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, escapeIdentifier(o.config.Database), escapeIdentifier(o.config.Table))
	} else {
		o.insertQuery = fmt.Sprintf(`
			INSERT INTO %s.%s (timestamp, metric, value, tags) VALUES (?, ?, ?, ?)
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

	// Cancel all flushes to enable graceful shutdown
	if o.shutdownCancel != nil {
		o.shutdownCancel()
	}

	// Stop scheduling new flushes
	if o.periodicFlusher != nil {
		o.periodicFlusher.Stop()
	}

	// Wait for all in-flight flushes to complete
	// This prevents closing the database while flushes are using it
	o.logger.Debug("Waiting for in-flight flushes to complete")
	o.flushWG.Wait()
	o.logger.Debug("All flushes completed")

	// Now safe to close database
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
	// Quick early exit check (before acquiring WaitGroup)
	o.mu.RLock()
	if o.closed {
		o.mu.RUnlock()
		return
	}

	// Register active flush while still under lock (prevents race with Stop())
	// This is critical: Add(1) must happen while holding RLock to prevent
	// Stop() from closing the database between the closed check and Add(1)
	o.flushWG.Add(1)

	// Capture state under lock
	db := o.db
	insertQuery := o.insertQuery
	config := o.config
	logger := o.logger
	ctx := o.shutdownCtx
	o.mu.RUnlock()

	// Ensure Done() is called even on early return
	defer o.flushWG.Done()

	// Check if context was cancelled during shutdown (if context is set)
	if ctx != nil {
		select {
		case <-ctx.Done():
			logger.Debug("Flush cancelled by shutdown context")
			return
		default:
		}
	}

	samples := o.GetBufferedSamples()
	if len(samples) == 0 {
		return
	}

	start := time.Now()

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
	totalSamples := 0

	// Calculate total samples for progress tracking
	for _, container := range samples {
		totalSamples += len(container.GetSamples())
	}

	for _, container := range samples {
		for _, sample := range container.GetSamples() {
			// Check for context cancellation every 1000 samples
			// Use non-blocking select for optimal performance
			if ctx != nil && count%1000 == 0 {
				select {
				case <-ctx.Done():
					logger.Warn("Flush cancelled by context",
						zap.Int("processed", count),
						zap.Int("total", totalSamples),
						zap.Error(ctx.Err()))
					return
				default:
					// Continue processing
				}
			}

			var execErr error

			if config.SchemaMode == "compatible" {
				cs, convErr := ConvertToCompatible(ctx, sample)
				if convErr != nil {
					logger.Error("failed to convert to compatible schema", zap.Error(convErr))
					// Return tag map to pool even on error
					tagMapPool.Put(cs.ExtraTags)
					continue
				}

				// Get row buffer from pool
				row := compatibleRowPool.Get().([]interface{})

				// Populate row buffer with sample data
				row[0] = cs.Timestamp
				row[1] = cs.BuildID
				row[2] = cs.Release
				row[3] = cs.Version
				row[4] = cs.Branch
				row[5] = cs.Metric
				row[6] = cs.MetricType
				row[7] = cs.Value
				row[8] = cs.TestID
				row[9] = cs.UIFeature
				row[10] = cs.Scenario
				row[11] = cs.Name
				row[12] = cs.Method
				row[13] = cs.Status
				row[14] = cs.ExpectedResponse
				row[15] = cs.ErrorCode
				row[16] = cs.Rating
				row[17] = cs.ResourceType
				row[18] = cs.CheckName
				row[19] = cs.GroupName
				row[20] = cs.ExtraTags

				_, execErr = stmt.ExecContext(ctx, row...)

				// Return pooled resources for reuse
				// Row buffer is always returned
				compatibleRowPool.Put(row)
				// Tag map is returned after ExecContext completes
				tagMapPool.Put(cs.ExtraTags)
			} else {
				ss := ConvertToSimple(ctx, sample)

				// Get row buffer from pool
				row := simpleRowPool.Get().([]interface{})

				// Populate row buffer with sample data
				row[0] = ss.Timestamp
				row[1] = ss.Metric
				row[2] = ss.Value
				row[3] = ss.Tags

				_, execErr = stmt.ExecContext(ctx, row...)

				// Return pooled resources for reuse
				// Row buffer is always returned
				simpleRowPool.Put(row)
				// Tag map is returned after ExecContext completes
				tagMapPool.Put(ss.Tags)
			}

			if execErr != nil {
				logger.Error("Failed to insert sample", zap.Error(execErr))
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
