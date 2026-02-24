package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/avast/retry-go/v4"
	"go.k6.io/k6/metrics"
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

// commitError wraps errors that occur during batch.Commit().
// Commit errors are ambiguous: the server may have persisted the data before the
// response was lost. To avoid duplication, these errors are NOT retried.
type commitError struct{ err error }

func (e *commitError) Error() string { return "commit error: " + e.err.Error() }
func (e *commitError) Unwrap() error { return e.err }

// escapeIdentifier escapes a ClickHouse identifier with backticks
func escapeIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "\\`") + "`"
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
	flushMu sync.Mutex     // Prevents overlapping flush cycles during outages

	// Context cancellation for graceful shutdown
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// Resilience: in-memory buffer for samples during connection failures
	failoverBuffer *SampleBuffer

	// Error metrics (atomic for lock-free concurrent access)
	convertErrors    atomic.Uint64 // Cumulative count of sample conversion failures
	insertErrors     atomic.Uint64 // Cumulative count of database insert failures
	samplesProcessed atomic.Uint64 // Cumulative count of successfully inserted samples

	// Resilience metrics (atomic for lock-free concurrent access)
	retryAttempts  atomic.Uint64 // Total retry attempts across all flushes
	flushFailures  atomic.Uint64 // Flushes that failed after all retries
	droppedSamples atomic.Uint64 // Samples dropped due to buffer overflow
}

// ErrorMetrics contains cumulative error statistics from flush operations.
// All counters are cumulative since output startup and are thread-safe.
type ErrorMetrics struct {
	// ConvertErrors is the total number of sample conversion failures.
	// These occur when a k6 sample cannot be transformed to a database row.
	ConvertErrors uint64

	// InsertErrors is the total number of database insert failures.
	// These occur when ExecContext fails for individual samples.
	InsertErrors uint64

	// SamplesProcessed is the total number of samples successfully inserted.
	SamplesProcessed uint64

	// RetryAttempts is the total number of retry attempts across all flushes.
	// High values indicate frequent transient connection issues.
	RetryAttempts uint64

	// FlushFailures is the count of flushes that failed after exhausting all retries.
	// These failures result in samples being buffered (if enabled) or lost.
	FlushFailures uint64

	// BufferedSamples is the current number of samples in the failover buffer.
	// Only populated when BufferEnabled is true.
	BufferedSamples uint64

	// DroppedSamples is the total number of samples dropped due to buffer overflow.
	// Only relevant when BufferEnabled is true.
	DroppedSamples uint64
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

	// Connect to ClickHouse without specifying database in auth.
	// This allows CREATE DATABASE IF NOT EXISTS to work when the target database doesn't exist.
	// All queries use fully-qualified table names ({database}.{table}), so no default database is needed.
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{o.config.Addr},
		Auth: clickhouse.Auth{
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

	// Initialize failover buffer if enabled
	if o.config.BufferEnabled {
		o.failoverBuffer = NewSampleBuffer(
			o.config.BufferMaxSamples,
			DropPolicy(o.config.BufferDropPolicy),
		)
		o.logger.Debug("Failover buffer initialized",
			zap.Int("capacity", o.config.BufferMaxSamples),
			zap.String("dropPolicy", o.config.BufferDropPolicy))
	}

	// Start periodic flusher
	pf, err := output.NewPeriodicFlusher(o.config.PushInterval, o.flush)
	if err != nil {
		return err
	}
	o.periodicFlusher = pf

	o.logger.Debug("Started",
		zap.Duration("interval", o.config.PushInterval),
		zap.Uint("retryAttempts", o.config.RetryAttempts),
		zap.Duration("retryDelay", o.config.RetryDelay),
		zap.Bool("bufferEnabled", o.config.BufferEnabled))
	return nil
}

// Stop flushes remaining metrics and closes the connection
func (o *Output) Stop() error {
	// Check if already stopped (read-only check to avoid blocking)
	o.mu.RLock()
	alreadyClosed := o.closed
	pf := o.periodicFlusher
	o.mu.RUnlock()

	if alreadyClosed {
		return nil
	}

	o.logger.Debug("Stopping")

	// Stop the periodic flusher FIRST — this triggers one final flush callback.
	// Since o.closed is still false, the final flush() executes normally.
	if pf != nil {
		pf.Stop()
	}

	// Now mark as closed to prevent any new flushes from starting.
	o.mu.Lock()
	if o.closed {
		// Another goroutine completed Stop() concurrently
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	o.mu.Unlock()

	// Wait for all in-flight flushes to complete (including the final one)
	o.logger.Debug("Waiting for in-flight flushes to complete")
	o.flushWG.Wait()
	o.logger.Debug("All flushes completed")

	// Final attempt to drain failover buffer before shutdown
	if o.failoverBuffer != nil && o.failoverBuffer.Len() > 0 {
		bufferedCount := o.failoverBuffer.Len()
		o.logger.Info("Draining failover buffer on shutdown",
			zap.Int("bufferedSamples", bufferedCount))

		// Use a fresh context for final drain (don't use cancelled shutdown context)
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer drainCancel()

		samples := o.failoverBuffer.PopAll()
		if len(samples) > 0 {
			if err := o.doFlush(drainCtx, samples); err != nil {
				o.logger.Warn("Failed to drain buffer on shutdown, data may be lost",
					zap.Int("lostSamples", len(samples)),
					zap.Error(err))
			} else {
				o.logger.Info("Successfully drained failover buffer",
					zap.Int("flushedSamples", len(samples)))
			}
		}
	}

	// Cancel shutdown context after final drain
	if o.shutdownCancel != nil {
		o.shutdownCancel()
	}

	// Now safe to close database
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.db != nil {
		_ = o.db.Close()
	}

	// Log final metrics
	errStats := o.GetErrorMetrics()
	o.logger.Info("ClickHouse output stopped",
		zap.Uint64("samplesProcessed", errStats.SamplesProcessed),
		zap.Uint64("convertErrors", errStats.ConvertErrors),
		zap.Uint64("insertErrors", errStats.InsertErrors),
		zap.Uint64("retryAttempts", errStats.RetryAttempts),
		zap.Uint64("flushFailures", errStats.FlushFailures),
		zap.Uint64("droppedSamples", errStats.DroppedSamples))

	return nil
}

// GetErrorMetrics returns cumulative error statistics from flush operations.
// All counters are thread-safe and can be called concurrently with flush operations.
func (o *Output) GetErrorMetrics() ErrorMetrics {
	var bufferedSamples uint64
	if o.failoverBuffer != nil {
		if n := o.failoverBuffer.Len(); n > 0 {
			bufferedSamples = uint64(n)
		}
	}

	return ErrorMetrics{
		ConvertErrors:    o.convertErrors.Load(),
		InsertErrors:     o.insertErrors.Load(),
		SamplesProcessed: o.samplesProcessed.Load(),
		RetryAttempts:    o.retryAttempts.Load(),
		FlushFailures:    o.flushFailures.Load(),
		BufferedSamples:  bufferedSamples,
		DroppedSamples:   o.droppedSamples.Load(),
	}
}

// isRetryableError checks if an error is transient and worth retrying.
// Connection errors, timeouts, and temporary network issues are retryable.
// Conversion errors and data validation errors are not.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Commit errors are never retryable — the server may have already persisted data
	var ce *commitError
	if errors.As(err, &ce) {
		return false
	}

	// Check for EOF errors using typed checks (avoids matching "thereof", "whereof", etc.)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for network errors (connection refused, timeout, etc.)
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Check for common ClickHouse connection error patterns
	errMsg := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"connection refused",
		"connection reset",
		"i/o timeout",
		"no such host",
		"network is unreachable",
		"broken pipe",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// flush writes buffered samples to ClickHouse with retry logic
func (o *Output) flush() {
	// Prevent overlapping flushes — if a previous flush is still running
	// (e.g., retrying during an outage), skip this cycle to avoid amplifying
	// load on an already-struggling ClickHouse.
	if !o.flushMu.TryLock() {
		return
	}
	defer o.flushMu.Unlock()

	// Quick early exit check (before acquiring WaitGroup)
	o.mu.RLock()
	if o.closed {
		o.mu.RUnlock()
		return
	}

	// Register active flush while still under lock (prevents race with Stop())
	o.flushWG.Add(1)

	// Capture state under lock
	ctx := o.shutdownCtx
	logger := o.logger
	retryAttempts := o.config.RetryAttempts
	retryDelay := o.config.RetryDelay
	retryMaxDelay := o.config.RetryMaxDelay
	bufferEnabled := o.config.BufferEnabled
	o.mu.RUnlock()

	defer o.flushWG.Done()

	// Check if context was cancelled during shutdown
	if ctx != nil {
		select {
		case <-ctx.Done():
			logger.Debug("Flush cancelled by shutdown context")
			return
		default:
		}
	}

	// Collect samples from both k6 buffer and failover buffer
	samples := o.GetBufferedSamples()

	// Also get any previously failed samples from failover buffer
	if o.failoverBuffer != nil {
		bufferedSamples := o.failoverBuffer.PopAll()
		if len(bufferedSamples) > 0 {
			logger.Debug("Recovered samples from failover buffer",
				zap.Int("count", len(bufferedSamples)))
			samples = append(bufferedSamples, samples...)
		}
	}

	if len(samples) == 0 {
		return
	}

	start := time.Now()

	// Wrap flush in retry logic
	err := retry.Do(
		func() error {
			return o.doFlush(ctx, samples)
		},
		retry.Attempts(retryAttempts+1), // +1 because Attempts includes the initial attempt
		retry.Delay(retryDelay),
		retry.MaxDelay(retryMaxDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.Context(ctx),
		retry.OnRetry(func(n uint, err error) {
			o.retryAttempts.Add(1)
			logger.Warn("Flush failed, retrying",
				zap.Uint("attempt", n+1),
				zap.Uint("maxAttempts", retryAttempts),
				zap.Error(err))
		}),
		retry.RetryIf(isRetryableError),
	)

	if err != nil {
		o.flushFailures.Add(1)
		logger.Error("Flush failed after retries",
			zap.Error(err),
			zap.Duration("elapsed", time.Since(start)))

		// Commit errors are ambiguous — data may already be persisted.
		// Do NOT buffer these samples to avoid duplication on next flush.
		var ce *commitError
		if errors.As(err, &ce) {
			logger.Warn("Commit error (data may already be persisted), not buffering samples",
				zap.Int("samples", len(samples)),
				zap.Error(err))
			return
		}

		// Buffer failed samples for later retry
		if bufferEnabled && o.failoverBuffer != nil {
			dropped := o.failoverBuffer.Push(samples)
			if dropped > 0 {
				o.droppedSamples.Add(uint64(dropped))
				logger.Warn("Buffer overflow, dropped samples",
					zap.Int("dropped", dropped),
					zap.Int("buffered", o.failoverBuffer.Len()))
			} else {
				logger.Info("Samples buffered for retry",
					zap.Int("count", len(samples)),
					zap.Int("bufferSize", o.failoverBuffer.Len()))
			}
		} else {
			logger.Error("Samples lost (buffering disabled)",
				zap.Int("lostSamples", len(samples)))
		}
	}
}

// doFlush performs the actual database insertion for a batch of samples.
// This is the core flush logic, separated to enable retry wrapping.
//
// Delivery semantics: at-least-once. If Commit() succeeds server-side but the
// response is lost, the caller receives a commitError (which is NOT retried).
// Samples are optimistically counted as processed before the commit error is returned,
// because they may already be persisted.
//
//nolint:gocyclo // complexity is acceptable for batch processing
func (o *Output) doFlush(ctx context.Context, samples []metrics.SampleContainer) error {
	o.mu.RLock()
	db := o.db
	insertQuery := o.insertQuery
	converter := o.converter
	logger := o.logger
	o.mu.RUnlock()

	if db == nil {
		return errors.New("database connection not initialized")
	}

	start := time.Now()

	// Begin transaction
	batch, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin batch: %w", err)
	}
	defer func() {
		if rollbackErr := batch.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			logger.Warn("failed to rollback transaction", zap.Error(rollbackErr))
		}
	}()

	stmt, err := batch.PrepareContext(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil {
			logger.Warn("failed to close statement", zap.Error(closeErr))
		}
	}()

	count := 0
	totalSamples := 0

	// Track conversion errors within this flush operation
	var flushConvertErrors uint64

	// Calculate total samples for progress tracking
	for _, container := range samples {
		totalSamples += len(container.GetSamples())
	}

	// Accumulate rows that were successfully passed to ExecContext.
	// These must NOT be released back to sync.Pool until after batch.Commit(),
	// because the ClickHouse driver holds references to row data internally.
	pendingRows := make([][]any, 0, totalSamples)
	defer func() {
		for _, row := range pendingRows {
			converter.Release(row)
		}
	}()

	for _, container := range samples {
		for _, sample := range container.GetSamples() {
			// Check for context cancellation every 1000 samples
			if ctx != nil && count%1000 == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}

			// Convert sample using the schema's converter
			row, convErr := converter.Convert(ctx, sample)
			if convErr != nil {
				flushConvertErrors++
				logger.Error("failed to convert sample", zap.Error(convErr))
				continue
			}

			// Execute insert — abort entire batch on first error.
			// The deferred batch.Rollback() handles cleanup.
			_, execErr := stmt.ExecContext(ctx, row...)
			if execErr != nil {
				converter.Release(row) // Driver discards failed rows, safe to release
				o.insertErrors.Add(1)
				if flushConvertErrors > 0 {
					o.convertErrors.Add(flushConvertErrors)
				}
				return fmt.Errorf("failed to insert sample: %w", execErr)
			}
			pendingRows = append(pendingRows, row)
			count++
		}
	}

	// If all samples had conversion errors, nothing to commit.
	// Conversion errors are deterministic — retrying won't help.
	if count == 0 {
		if flushConvertErrors > 0 {
			o.convertErrors.Add(flushConvertErrors)
			logger.Warn("All samples failed conversion, skipping commit",
				zap.Uint64("convertErrors", flushConvertErrors),
				zap.Int("totalSamples", totalSamples))
		}
		return nil
	}

	if err := batch.Commit(); err != nil {
		// Commit errors are ambiguous: data may already be persisted server-side.
		// Optimistically count samples as processed and wrap as commitError
		// so retry logic does NOT re-insert (avoiding duplication).
		if flushConvertErrors > 0 {
			o.convertErrors.Add(flushConvertErrors)
		}
		o.samplesProcessed.Add(uint64(count))
		return &commitError{err: err}
	}

	// Update cumulative atomic counters
	if flushConvertErrors > 0 {
		o.convertErrors.Add(flushConvertErrors)
	}
	o.samplesProcessed.Add(uint64(count))

	// Log summary
	if flushConvertErrors > 0 {
		logger.Warn("Flush completed with conversion errors",
			zap.Uint64("convertErrors", flushConvertErrors),
			zap.Int("successfulInserts", count),
			zap.Int("totalSamples", totalSamples),
			zap.Duration("elapsed", time.Since(start)))
	} else {
		logger.Debug("Flushed metrics",
			zap.Int("samples", count),
			zap.Duration("elapsed", time.Since(start)))
	}

	return nil
}
