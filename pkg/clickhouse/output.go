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
	"go.uber.org/zap/zapcore"
)

// Memory pools for reducing allocations during high-throughput operations
var (
	// tagMapPool reuses map[string]string for tag storage
	// Maps are cleared before returning to pool to prevent memory leaks
	tagMapPool = sync.Pool{
		New: func() any {
			return make(map[string]string)
		},
	}

	// compatibleRowPool reuses []any slices for compatible schema rows (21 fields)
	// Pre-sized to avoid slice growth during append operations
	compatibleRowPool = sync.Pool{
		New: func() any {
			return make([]any, 21)
		},
	}

	// simpleRowPool reuses []any slices for simple schema rows (4 fields)
	// Pre-sized to match simple schema field count
	simpleRowPool = sync.Pool{
		New: func() any {
			return make([]any, 4)
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

	// Schema implementation (selected by schemaMode config)
	schema    SchemaCreator
	converter SampleConverter

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

	// Create production logger with ISO 8601 timestamps
	logCfg := zap.NewProductionConfig()
	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, err := logCfg.Build()
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
			o.logger.Debug("TLS enabled with certificate verification")
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
	if err := db.PingContext(o.shutdownCtx); err != nil {
		return fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	o.db = db
	o.logger.Debug("Connected to ClickHouse")

	// Get schema implementation from registry
	impl, err := GetSchema(o.config.SchemaMode)
	if err != nil {
		return fmt.Errorf("failed to get schema implementation: %w", err)
	}
	o.schema = impl.Schema
	o.converter = impl.Converter
	o.logger.Debug("Using schema implementation", zap.String("schemaMode", o.config.SchemaMode))

	// Create schema if not skipped
	if !o.config.SkipSchemaCreation {
		if err := o.schema.CreateSchema(o.shutdownCtx, db, o.config.Database, o.config.Table); err != nil {
			return err
		}
		o.logger.Debug("Schema created")
	} else {
		o.logger.Debug("Schema creation skipped")
	}

	// Pre-compute INSERT query from schema implementation
	o.insertQuery = o.schema.InsertQuery(o.config.Database, o.config.Table)

	// Start periodic flusher
	pf, err := output.NewPeriodicFlusher(o.config.PushInterval, o.flush)
	if err != nil {
		return err
	}
	o.periodicFlusher = pf

	o.logger.Debug("Started", zap.Duration("interval", o.config.PushInterval))
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
		_ = o.db.Close()
	}

	o.logger.Debug("Stopped")
	return nil
}

// flush writes buffered samples to ClickHouse
//
//nolint:gocyclo // complexity is acceptable for batch processing with error handling
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
	converter := o.converter
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
	batch, err := db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error("Failed to begin batch", zap.Error(err))
		return
	}
	defer func() { _ = batch.Rollback() }()

	stmt, err := batch.PrepareContext(ctx, insertQuery)
	if err != nil {
		logger.Error("Failed to prepare statement", zap.Error(err))
		return
	}
	defer func() { _ = stmt.Close() }()

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

			// Convert sample using the schema's converter
			row, convErr := converter.Convert(ctx, sample)
			if convErr != nil {
				logger.Error("failed to convert sample", zap.Error(convErr))
				continue
			}

			// Execute insert
			_, execErr := stmt.ExecContext(ctx, row...)

			// Release pooled resources (row buffer and tag maps)
			converter.Release(row)

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
