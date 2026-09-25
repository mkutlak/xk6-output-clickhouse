package clickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
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

// errNotStarted is returned when writing before Start connected to ClickHouse.
// It is not retryable.
var errNotStarted = errors.New("output not started")

// escapeIdentifier escapes a ClickHouse identifier with backticks
func escapeIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "\\`") + "`"
}

// clickhouseOutput implements k6's output.Output interface
type clickhouseOutput struct {
	output.SampleBuffer
	config          config
	logger          logrus.FieldLogger
	db              *sql.DB
	periodicFlusher *output.PeriodicFlusher
	insertQuery     string // Pre-computed INSERT query

	// table is the quoted `database`.`table` identifier, computed once in New.
	table string

	// tlsConfig is built once in New from config.TLS; nil when TLS is disabled.
	tlsConfig *tls.Config

	// Schema selected by the schemaMode config
	schema Schema

	stopOnce sync.Once

	// pending holds samples from failed flushes, retried first on the next
	// flush. Only touched by the flusher goroutine and by Stop after the flusher
	// stopped.
	pending []metrics.Sample

	// stats are written only by the flusher goroutine, and read by Stop
	// after the flusher has stopped.
	stats struct {
		written, convertErrors, insertErrors, retries, flushFailures, dropped uint64
	}
}

// Compile-time assertion that *clickhouseOutput satisfies k6's output.Output interface.
// AddMetricSamples is promoted from the embedded output.SampleBuffer; this makes an
// accidental break surface here rather than at the RegisterExtension call site.
var _ output.Output = (*clickhouseOutput)(nil)

// New creates a new ClickHouse output
func New(params output.Params) (output.Output, error) {
	cfg, err := parseConfig(params)
	if err != nil {
		return nil, err
	}

	logger := params.Logger
	if logger == nil {
		logger = logrus.New()
	}
	logger = logger.WithField("output", "clickhouse")

	if unknown := unknownEnvVars(params.Environment); len(unknown) > 0 {
		logger.Warnf("Ignoring unknown environment variables: %s", strings.Join(unknown, ", "))
	}

	// Resolve everything config-derived once, so Start (and any config-only
	// inspection before it) never re-reads TLS files or re-looks-up the schema.
	schema, err := getSchema(cfg.SchemaMode)
	if err != nil {
		return nil, err
	}

	tlsConfig, err := cfg.TLS.build()
	if err != nil {
		return nil, fmt.Errorf("invalid TLS configuration: %w", err)
	}

	table := escapeIdentifier(cfg.Database) + "." + escapeIdentifier(cfg.Table)

	o := &clickhouseOutput{
		config:      cfg,
		logger:      logger,
		schema:      schema,
		tlsConfig:   tlsConfig,
		table:       table,
		insertQuery: schema.InsertQuery(table),
	}
	o.logTLSStatus()

	return o, nil
}

// Description returns a human-readable description
func (o *clickhouseOutput) Description() string {
	return fmt.Sprintf("clickhouse (%s, %s.%s, schema=%s)",
		o.config.Addr, o.config.Database, o.config.Table, o.config.SchemaMode)
}

// Start connects to ClickHouse, creates the schema unless skipped, and starts
// the periodic flusher.
func (o *clickhouseOutput) Start() (err error) {
	// Connect to ClickHouse without specifying database in auth.
	// This allows CREATE DATABASE IF NOT EXISTS to work when the target database doesn't exist.
	// All queries use fully-qualified table names ({database}.{table}), so no default database is needed.
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{o.config.Addr},
		Auth: clickhouse.Auth{
			Username: o.config.User,
			Password: o.config.Password,
		},
		TLS: o.tlsConfig,
	})
	defer func() {
		if err != nil {
			_ = db.Close()
			o.db = nil
		}
	}()

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to clickhouse at %s: %w "+
			"(verify the address and the native port — 9000 by default, not the 8123 HTTP port — and the credentials)",
			o.config.Addr, err)
	}

	if !o.config.SkipSchemaCreation {
		if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+escapeIdentifier(o.config.Database)); err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
		if _, err := db.ExecContext(ctx, o.schema.CreateTable(o.table)); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// The flusher goroutine reads o.db; it must be set before it starts.
	o.db = db

	pf, err := output.NewPeriodicFlusher(o.config.PushInterval, o.flush)
	if err != nil {
		return err
	}
	o.periodicFlusher = pf

	o.logger.WithFields(logrus.Fields{
		"addr":             o.config.Addr,
		"database":         o.config.Database,
		"table":            o.config.Table,
		"schemaMode":       o.config.SchemaMode,
		"pushInterval":     o.config.PushInterval,
		"retryAttempts":    o.config.RetryAttempts,
		"bufferEnabled":    o.config.BufferEnabled,
		"bufferMaxSamples": o.config.BufferMaxSamples,
	}).Debug("Started")
	return nil
}

// logTLSStatus logs warnings about the TLS configuration: using the plaintext
// port with TLS, verification being disabled, and TLS material that will be
// silently ignored. Called from New so these surface as soon as the output is
// constructed, before Start attempts a connection.
func (o *clickhouseOutput) logTLSStatus() {
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

// drainTimeout bounds the final attempt to write pending samples on Stop.
const drainTimeout = 30 * time.Second

// Stop flushes remaining metrics and closes the connection. Only the first
// call has an effect.
func (o *clickhouseOutput) Stop() error {
	o.stopOnce.Do(o.stop)
	return nil
}

func (o *clickhouseOutput) stop() {
	o.logger.Debug("Stopping")

	// Runs one final flush on the flusher goroutine and waits for it.
	if o.periodicFlusher != nil {
		o.periodicFlusher.Stop()
	}

	// Final attempt to drain pending samples before shutdown. write retries
	// with the normal backoff policy: the outage that filled the buffer may
	// still be flapping.
	if len(o.pending) > 0 {
		samples := o.pending
		o.pending = nil
		o.logger.WithField("bufferedSamples", len(samples)).Info("Draining pending samples on shutdown")

		drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
		kept, err := o.write(drainCtx, samples)
		drainCancel()
		switch {
		case err == nil:
			o.logger.WithField("flushedSamples", len(kept)).Info("Successfully drained pending samples")
		case isCommitError(err):
			// Commit errors are ambiguous — the server may already hold the data.
			// Don't count them as dropped (mirrors flush()).
			o.logger.WithError(err).WithField("samples", len(kept)).Warn("Commit error during shutdown drain (data may already be persisted)")
		default:
			o.stats.dropped += uint64(len(kept))
			o.logger.WithError(err).WithField("lostSamples", len(kept)).Warn("Failed to drain buffer on shutdown, data lost")
		}
	}

	if o.db != nil {
		_ = o.db.Close()
	}

	s := o.stats
	log := o.logger.WithFields(logrus.Fields{
		"samplesProcessed": s.written,
		"convertErrors":    s.convertErrors,
		"insertErrors":     s.insertErrors,
		"retryAttempts":    s.retries,
		"flushFailures":    s.flushFailures,
		"droppedSamples":   s.dropped,
	})
	if s.dropped > 0 || s.convertErrors > 0 || s.flushFailures > 0 {
		log.Warn("ClickHouse output stopped")
	} else {
		log.Info("ClickHouse output stopped")
	}
}

// isRetryableError checks if an error is transient and worth retrying.
// Connection errors, timeouts, and temporary network issues are retryable.
// Commit errors, errNotStarted and data validation errors are not.
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

// flush writes buffered samples to ClickHouse with retry logic. It runs only on
// the periodic flusher goroutine, one call at a time.
func (o *clickhouseOutput) flush() {
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
	kept, err := o.write(context.Background(), samples)
	if err == nil {
		o.logger.WithFields(logrus.Fields{
			"samples": len(kept),
			"elapsed": time.Since(start),
		}).Debug("Flushed samples")
		return
	}

	o.stats.flushFailures++
	log := o.logger.WithError(err).WithField("elapsed", time.Since(start))
	switch {
	case isCommitError(err):
		// Commit errors are ambiguous — data may already be persisted.
		// Do NOT buffer these samples to avoid duplication on next flush.
		log.WithField("samples", len(kept)).Warn("Flush commit failed (data may already be persisted), not buffering samples")
	case !o.config.BufferEnabled:
		o.stats.dropped += uint64(len(kept))
		log.WithField("lostSamples", len(kept)).Error("Flush failed, samples lost (buffering disabled)")
	default:
		var dropped int
		o.pending, dropped = bound(kept, o.config.BufferMaxSamples, o.config.BufferDropPolicy)
		o.stats.dropped += uint64(dropped)
		log.WithFields(logrus.Fields{
			"buffered": len(o.pending),
			"dropped":  dropped,
		}).Warn("Flush failed, samples buffered for retry")
	}
}

// write converts samples to rows once, then inserts the rows, retrying
// transient database errors. Samples that fail conversion are counted and
// discarded. It returns the converted samples, for re-buffering on error.
func (o *clickhouseOutput) write(ctx context.Context, samples []metrics.Sample) ([]metrics.Sample, error) {
	ok := samples[:0]
	rows := make([][]any, 0, len(samples))
	var convertErrors uint64
	var firstErr error
	for _, sample := range samples {
		row, err := o.schema.Row(sample)
		if err != nil {
			if convertErrors == 0 {
				firstErr = err
			}
			convertErrors++
			continue
		}
		ok = append(ok, sample)
		rows = append(rows, row)
	}
	clear(samples[len(ok):])

	if convertErrors > 0 {
		o.stats.convertErrors += convertErrors
		o.logger.WithError(firstErr).WithFields(logrus.Fields{
			"convertErrors": convertErrors,
			"totalSamples":  len(samples),
		}).Warn("Failed to convert samples, skipping them")
	}
	if len(rows) == 0 {
		return nil, nil
	}

	attempts := o.config.RetryAttempts + 1 // the initial attempt plus retries
	err := retry.Do(
		func() error { return o.insertRows(ctx, rows) },
		retry.Attempts(attempts),
		retry.Delay(o.config.RetryDelay),
		retry.MaxDelay(o.config.RetryMaxDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.Context(ctx),
		retry.RetryIf(isRetryableError),
		retry.OnRetry(func(n uint, err error) {
			// retry-go also calls this after the final attempt, which is not
			// followed by a retry.
			if n+1 == attempts {
				return
			}
			o.stats.retries++
			o.logger.WithError(err).WithFields(logrus.Fields{
				"attempt":     n + 1,
				"maxAttempts": attempts,
			}).Warn("Flush failed, retrying")
		}),
	)
	return ok, err
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

// insertRows inserts rows in a single transaction.
//
// Delivery semantics: at-least-once. If Commit() succeeds server-side but the
// response is lost, the caller receives a commitError (which is NOT retried).
// Rows are optimistically counted as written before the commit error is
// returned, because they may already be persisted.
func (o *clickhouseOutput) insertRows(ctx context.Context, rows [][]any) error {
	if o.db == nil {
		return errNotStarted
	}

	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin batch: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			o.logger.WithError(rollbackErr).Warn("Failed to rollback transaction")
		}
	}()

	stmt, err := tx.PrepareContext(ctx, o.insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil {
			o.logger.WithError(closeErr).Warn("Failed to close statement")
		}
	}()

	// Abort the whole batch on the first error; the deferred Rollback cleans up.
	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			o.stats.insertErrors++
			return fmt.Errorf("failed to insert sample: %w", err)
		}
	}

	o.stats.written += uint64(len(rows))
	if err := tx.Commit(); err != nil {
		return &commitError{err: err}
	}
	return nil
}
