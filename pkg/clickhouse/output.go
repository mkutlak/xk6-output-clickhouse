package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/avast/retry-go/v4"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// commitError wraps errors that occur during batch.Commit().
// Commit errors are ambiguous: the server may have persisted the data before the
// response was lost. To avoid duplication, these errors are NOT retried.
type commitError struct{ err error }

func (e *commitError) Error() string { return "commit error: " + e.err.Error() }
func (e *commitError) Unwrap() error { return e.err }

// isCommitError reports whether err is (or wraps) a commitError.
func isCommitError(err error) bool {
	_, ok := errors.AsType[*commitError](err)
	return ok
}

// escapeIdentifier escapes a ClickHouse identifier with backticks
func escapeIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "\\`") + "`"
}

// Output implements the output.Output interface
type Output struct {
	output.SampleBuffer
	config          Config
	logger          logrus.FieldLogger
	db              *sql.DB
	periodicFlusher *output.PeriodicFlusher
	insertQuery     string // Pre-computed INSERT query

	// Schema selected by the schemaMode config
	schema Schema

	// Concurrency control
	mu      sync.RWMutex
	closed  bool
	flushWG sync.WaitGroup // Track in-flight flushes
	flushMu sync.Mutex     // Prevents overlapping flush cycles during outages

	// Context cancellation for graceful shutdown
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// pending holds failed samples waiting for the next flush; only touched by
	// flush() and by Stop() after flushes finished.
	pending []metrics.Sample

	// Error metrics (atomic for lock-free concurrent access)
	convertErrors    atomic.Uint64 // Cumulative count of sample conversion failures
	insertErrors     atomic.Uint64 // Cumulative count of database insert failures
	samplesProcessed atomic.Uint64 // Cumulative count of successfully inserted samples

	// Resilience metrics (atomic for lock-free concurrent access)
	retryAttempts  atomic.Uint64 // Total retry attempts across all flushes
	flushFailures  atomic.Uint64 // Flushes that failed after all retries
	droppedSamples atomic.Uint64 // Samples dropped: overflow, buffering disabled, or lost at shutdown
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

	// DroppedSamples is the total number of samples dropped: buffer overflow,
	// buffering disabled, or undrainable at shutdown.
	DroppedSamples uint64
}

// Compile-time assertion that *Output satisfies k6's output.Output interface.
// AddMetricSamples is promoted from the embedded output.SampleBuffer; this makes an
// accidental break surface here rather than at the RegisterExtension call site.
var _ output.Output = (*Output)(nil)

// New creates a new ClickHouse output
func New(params output.Params) (output.Output, error) {
	cfg, err := ParseConfig(params)
	if err != nil {
		return nil, err
	}

	logger := params.Logger
	if logger == nil {
		logger = logrus.New()
	}

	return &Output{
		config: cfg,
		logger: logger.WithField("output", "clickhouse"),
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
	o.shutdownCtx, o.shutdownCancel = context.WithCancel(context.Background()) // #nosec G118 -- shutdownCancel is called in Stop()

	o.logger.Debug("Starting ClickHouse output")

	// Build TLS configuration
	tlsConfig, err := o.config.TLS.BuildTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to build TLS config: %w", err)
	}

	o.logTLSStatus()

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
		return fmt.Errorf("failed to connect to clickhouse at %s: %w "+
			"(verify the address and the native port — 9000 by default, not the 8123 HTTP port — and the credentials)",
			o.config.Addr, err)
	}

	o.db = db
	o.logger.Debug("Connected to ClickHouse")

	// Get schema implementation from registry
	schema, err := getSchema(o.config.SchemaMode)
	if err != nil {
		return fmt.Errorf("failed to get schema implementation: %w", err)
	}
	o.schema = schema
	o.logger.WithField("schemaMode", o.config.SchemaMode).Debug("Using schema implementation")

	table := escapeIdentifier(o.config.Database) + "." + escapeIdentifier(o.config.Table)

	// Create database and table if not skipped
	if !o.config.SkipSchemaCreation {
		if _, err := db.ExecContext(o.shutdownCtx, "CREATE DATABASE IF NOT EXISTS "+escapeIdentifier(o.config.Database)); err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
		if _, err := db.ExecContext(o.shutdownCtx, schema.CreateTable(table)); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		o.logger.Debug("Schema created")
	} else {
		o.logger.Debug("Schema creation skipped")
	}

	// Pre-compute INSERT query from schema implementation
	o.insertQuery = schema.InsertQuery(table)

	// Start periodic flusher
	pf, err := output.NewPeriodicFlusher(o.config.PushInterval, o.flush)
	if err != nil {
		return err
	}
	o.periodicFlusher = pf

	o.logger.WithFields(logrus.Fields{
		"interval":      o.config.PushInterval,
		"retryAttempts": o.config.RetryAttempts,
		"retryDelay":    o.config.RetryDelay,
		"bufferEnabled": o.config.BufferEnabled,
	}).Debug("Started")
	return nil
}

// logTLSStatus logs warnings about the TLS configuration: using the plaintext
// port with TLS, verification being disabled, and TLS material that will be
// silently ignored. Extracted from Start() to keep its complexity in check.
func (o *Output) logTLSStatus() {
	if !o.config.TLS.Enabled {
		// Surface silently-ignored TLS material so a forgotten tlsEnabled doesn't
		// leave the connection unencrypted while certs are configured.
		if o.config.TLS.CAFile != "" || o.config.TLS.CertFile != "" || o.config.TLS.KeyFile != "" {
			o.logger.Warn("TLS certificate/CA files are configured but TLS is disabled; they will be ignored. Set tlsEnabled=true (or K6_CLICKHOUSE_TLS_ENABLED=true) to use them.")
		}
		o.logger.Debug("TLS disabled, using unencrypted connection")
		return
	}

	// Warn if using the plaintext native port 9000 with TLS (should be 9440).
	if strings.Contains(o.config.Addr, ":9000") {
		o.logger.Warn("TLS is enabled but using port 9000. Consider using port 9440 for secure connections.")
	}

	if o.config.TLS.InsecureSkipVerify {
		o.logger.Warn("TLS enabled with InsecureSkipVerify=true. Certificate verification is DISABLED. This is insecure and should only be used for testing.")
		// With verification disabled, RootCAs and ServerName are not consulted —
		// warn so the user isn't misled into thinking the CA/SNI is enforced.
		if o.config.TLS.CAFile != "" || o.config.TLS.ServerName != "" {
			o.logger.Warn("InsecureSkipVerify=true overrides certificate verification: the configured CA file and serverName are ignored.")
		}
		return
	}

	o.logger.Debug("TLS enabled with certificate verification")
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

	// Final attempt to drain pending samples before shutdown
	if len(o.pending) > 0 {
		samples := o.pending
		o.logger.WithField("bufferedSamples", len(samples)).Info("Draining pending samples on shutdown")

		// Use a fresh context for final drain (don't use cancelled shutdown context)
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer drainCancel()

		// Retry the final drain with the same backoff policy as a normal flush.
		// The outage that filled the buffer may still be flapping, so a single
		// unretried attempt would needlessly lose data inside the 30s window.
		// config is immutable after New(); no flushes are in flight here (we
		// already waited on flushWG), so reading it without the lock is safe.
		err := retry.Do(
			func() error { return o.doFlush(drainCtx, samples) },
			retry.Attempts(o.config.RetryAttempts+1),
			retry.Delay(o.config.RetryDelay),
			retry.MaxDelay(o.config.RetryMaxDelay),
			retry.DelayType(retry.BackOffDelay),
			retry.Context(drainCtx),
			retry.RetryIf(isRetryableError),
		)
		switch {
		case err == nil:
			o.logger.WithField("flushedSamples", len(samples)).Info("Successfully drained pending samples")
		case isCommitError(err):
			// Commit errors are ambiguous — the server may already hold the data.
			// Don't count them as dropped (mirrors flush()).
			o.logger.WithError(err).WithField("samples", len(samples)).Warn("Commit error during shutdown drain (data may already be persisted)")
		default:
			// Unrecoverable at shutdown; count the loss so the final metrics
			// summary is accurate instead of silently under-reporting drops.
			o.droppedSamples.Add(uint64(len(samples)))
			o.logger.WithError(err).WithField("lostSamples", len(samples)).Warn("Failed to drain buffer on shutdown, data lost")
		}

		o.pending = nil
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
	o.logger.WithFields(logrus.Fields{
		"samplesProcessed": errStats.SamplesProcessed,
		"convertErrors":    errStats.ConvertErrors,
		"insertErrors":     errStats.InsertErrors,
		"retryAttempts":    errStats.RetryAttempts,
		"flushFailures":    errStats.FlushFailures,
		"droppedSamples":   errStats.DroppedSamples,
	}).Info("ClickHouse output stopped")

	return nil
}

// GetErrorMetrics returns cumulative error statistics from flush operations.
// All counters are thread-safe and can be called concurrently with flush operations.
func (o *Output) GetErrorMetrics() ErrorMetrics {
	return ErrorMetrics{
		ConvertErrors:    o.convertErrors.Load(),
		InsertErrors:     o.insertErrors.Load(),
		SamplesProcessed: o.samplesProcessed.Load(),
		RetryAttempts:    o.retryAttempts.Load(),
		FlushFailures:    o.flushFailures.Load(),
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
	if isCommitError(err) {
		return false
	}

	// Check for EOF errors using typed checks (avoids matching "thereof", "whereof", etc.)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for network errors (connection refused, timeout, etc.)
	if _, ok := errors.AsType[net.Error](err); ok {
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

	// Previously failed samples go first, followed by new samples flattened
	// from k6's per-container buffer.
	samples := o.pending
	o.pending = nil
	for _, c := range o.GetBufferedSamples() {
		samples = append(samples, c.GetSamples()...)
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
			logger.WithError(err).WithFields(logrus.Fields{
				// Total attempt budget is retryAttempts+1 (initial + retries);
				// report that so "attempt" never exceeds "maxAttempts".
				"attempt":     n + 1,
				"maxAttempts": retryAttempts + 1,
			}).Warn("Flush failed, retrying")
		}),
		retry.RetryIf(isRetryableError),
	)

	if err != nil {
		o.flushFailures.Add(1)
		logger.WithError(err).WithField("elapsed", time.Since(start)).Error("Flush failed after retries")

		switch {
		case isCommitError(err):
			// Commit errors are ambiguous — data may already be persisted.
			// Do NOT buffer these samples to avoid duplication on next flush.
			logger.WithError(err).WithField("samples", len(samples)).Warn("Commit error (data may already be persisted), not buffering samples")
		case !bufferEnabled:
			o.droppedSamples.Add(uint64(len(samples)))
			logger.WithField("lostSamples", len(samples)).Error("Samples lost (buffering disabled)")
		default:
			var dropped int
			o.pending, dropped = bound(samples, o.config.BufferMaxSamples, o.config.BufferDropPolicy)
			if dropped > 0 {
				o.droppedSamples.Add(uint64(dropped))
				logger.WithFields(logrus.Fields{
					"dropped":  dropped,
					"buffered": len(o.pending),
				}).Warn("Buffer overflow, dropped samples")
			} else {
				logger.WithFields(logrus.Fields{
					"count":      len(o.pending),
					"bufferSize": len(o.pending),
				}).Info("Samples buffered for retry")
			}
		}
	}
}

// bound trims samples to at most limit, dropping the oldest or the newest
// according to policy, and reports how many were dropped.
func bound(samples []metrics.Sample, limit int, policy string) (kept []metrics.Sample, dropped int) {
	n := len(samples) - limit
	switch {
	case n <= 0:
		return samples, 0
	case policy == dropNewest:
		clear(samples[limit:])
		return samples[:limit], n
	default:
		return slices.Delete(samples, 0, n), n
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
func (o *Output) doFlush(ctx context.Context, samples []metrics.Sample) error {
	o.mu.RLock()
	db := o.db
	insertQuery := o.insertQuery
	schema := o.schema
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
			logger.WithError(rollbackErr).Warn("Failed to rollback transaction")
		}
	}()

	stmt, err := batch.PrepareContext(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil {
			logger.WithError(closeErr).Warn("Failed to close statement")
		}
	}()

	count := 0
	totalSamples := len(samples)

	// Track conversion errors within this flush operation.
	// Deferred so every return path (including context cancellation) flushes the counter.
	var flushConvertErrors uint64
	defer func() {
		if flushConvertErrors > 0 {
			o.convertErrors.Add(flushConvertErrors)
		}
	}()

	for _, sample := range samples {
		// Check for context cancellation every 1000 samples
		if ctx != nil && count%1000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		// Convert sample into a row for the schema
		row, convErr := schema.Row(sample)
		if convErr != nil {
			flushConvertErrors++
			logger.WithError(convErr).Warn("Failed to convert sample")
			continue
		}

		// Execute insert — abort entire batch on first error.
		// The deferred batch.Rollback() handles cleanup.
		_, execErr := stmt.ExecContext(ctx, row...)
		if execErr != nil {
			o.insertErrors.Add(1)
			return fmt.Errorf("failed to insert sample: %w", execErr)
		}
		count++
	}

	// If all samples had conversion errors, nothing to commit.
	// Conversion errors are deterministic — retrying won't help.
	if count == 0 {
		if flushConvertErrors > 0 {
			logger.WithFields(logrus.Fields{
				"convertErrors": flushConvertErrors,
				"totalSamples":  totalSamples,
			}).Warn("All samples failed conversion, skipping commit")
		}
		return nil
	}

	if err := batch.Commit(); err != nil {
		// Commit errors are ambiguous: data may already be persisted server-side.
		// Optimistically count samples as processed and wrap as commitError
		// so retry logic does NOT re-insert (avoiding duplication).
		o.samplesProcessed.Add(uint64(count))
		return &commitError{err: err}
	}

	o.samplesProcessed.Add(uint64(count))

	// Log summary
	if flushConvertErrors > 0 {
		logger.WithFields(logrus.Fields{
			"convertErrors":     flushConvertErrors,
			"successfulInserts": count,
			"totalSamples":      totalSamples,
			"elapsed":           time.Since(start),
		}).Warn("Flush completed with conversion errors")
	} else {
		logger.WithFields(logrus.Fields{
			"samples": count,
			"elapsed": time.Since(start),
		}).Debug("Flushed metrics")
	}

	return nil
}
