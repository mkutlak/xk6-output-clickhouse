package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/avast/retry-go/v4"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		params  output.Params
		wantErr string // substring; empty means New must succeed
		check   func(t *testing.T, o *clickhouseOutput)
	}{
		{
			name:   "defaults",
			params: output.Params{Logger: newTestLogger(t)},
			check: func(t *testing.T, o *clickhouseOutput) {
				assert.Equal(t, "localhost:9000", o.config.Addr)
			},
		},
		{
			name: "json config",
			params: output.Params{
				Logger: newTestLogger(t),
				JSONConfig: mustMarshalJSON(map[string]any{
					"addr":         "clickhouse:9000",
					"database":     "metrics",
					"table":        "k6_samples",
					"pushInterval": "5s",
				}),
			},
			check: func(t *testing.T, o *clickhouseOutput) {
				assert.Equal(t, "clickhouse:9000", o.config.Addr)
				assert.Equal(t, "metrics", o.config.Database)
				assert.Equal(t, "k6_samples", o.config.Table)
				assert.Equal(t, 5*time.Second, o.config.PushInterval)
			},
		},
		{
			name: "url config",
			params: output.Params{
				Logger:         newTestLogger(t),
				ConfigArgument: "localhost:9000?database=test&table=samples",
			},
			check: func(t *testing.T, o *clickhouseOutput) {
				assert.Equal(t, "test", o.config.Database)
				assert.Equal(t, "samples", o.config.Table)
			},
		},
		{
			name: "bad json",
			params: output.Params{
				Logger:     newTestLogger(t),
				JSONConfig: []byte(`{invalid`),
			},
			wantErr: "json config: invalid character",
		},
		{
			name: "invalid option value",
			params: output.Params{
				Logger: newTestLogger(t),
				JSONConfig: mustMarshalJSON(map[string]any{
					"pushInterval": "not-a-duration",
				}),
			},
			wantErr: "invalid pushInterval",
		},
		{
			name: "unknown option",
			params: output.Params{
				Logger:     newTestLogger(t),
				JSONConfig: mustMarshalJSON(map[string]any{"databse": "k6"}),
			},
			wantErr: `unknown option "databse"`,
		},
		{
			name: "unknown schema mode",
			params: output.Params{
				Logger:     newTestLogger(t),
				JSONConfig: mustMarshalJSON(map[string]any{"schemaMode": "nope"}),
			},
			wantErr: `unknown schemaMode "nope"`,
		},
		{
			name: "bad TLS file",
			params: output.Params{
				Logger: newTestLogger(t),
				JSONConfig: mustMarshalJSON(map[string]any{
					"tlsEnabled": true,
					"tlsCAFile":  "/nonexistent/ca.pem",
				}),
			},
			wantErr: "invalid TLS configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := New(tt.params)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, out)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)

			o, ok := out.(*clickhouseOutput)
			require.True(t, ok, "output should be of type *clickhouseOutput")
			assert.NotNil(t, o.logger)
			if tt.check != nil {
				tt.check(t, o)
			}
		})
	}
}

func TestNew_LoggerFallback(t *testing.T) {
	t.Parallel()

	t.Run("uses the provided logger", func(t *testing.T) {
		t.Parallel()
		out, err := New(output.Params{Logger: newTestLogger(t)})
		require.NoError(t, err)
		assert.NotNil(t, out.(*clickhouseOutput).logger)
	})

	t.Run("falls back to a default logger when none is provided", func(t *testing.T) {
		t.Parallel()
		out, err := New(output.Params{})
		require.NoError(t, err)
		assert.NotNil(t, out.(*clickhouseOutput).logger)
	})
}

func TestNew_WarnsOnUnknownEnvVars(t *testing.T) {
	t.Parallel()

	logger, hook := logrustest.NewNullLogger()
	_, err := New(output.Params{
		Logger: logger,
		Environment: map[string]string{
			"K6_CLICKHOUSE_DB":       "k6",
			"K6_CLICKHOUSE_DATABASE": "metrics",
		},
	})
	require.NoError(t, err)
	require.Len(t, hook.AllEntries(), 1)
	assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
	assert.Equal(t, "Ignoring unknown environment variables: K6_CLICKHOUSE_DATABASE", hook.LastEntry().Message)
}

func TestOutput_Description(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   config
		expected string
	}{
		{
			name: "default config",
			config: config{
				Addr:       "localhost:9000",
				Database:   "k6",
				Table:      "samples",
				SchemaMode: "simple",
			},
			expected: "clickhouse (localhost:9000, k6.samples, schema=simple)",
		},
		{
			name: "custom config",
			config: config{
				Addr:       "clickhouse.example.com:9000",
				Database:   "production",
				Table:      "metrics",
				SchemaMode: "compatible",
			},
			expected: "clickhouse (clickhouse.example.com:9000, production.metrics, schema=compatible)",
		},
		{
			name: "ipv6 address",
			config: config{
				Addr:       "[::1]:9000",
				Database:   "test",
				Table:      "samples",
				SchemaMode: "simple",
			},
			expected: "clickhouse ([::1]:9000, test.samples, schema=simple)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := &clickhouseOutput{config: tt.config}
			assert.Equal(t, tt.expected, out.Description())
		})
	}
}

func TestIsRetryableError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"io.EOF", io.EOF, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"wrapped io.EOF", fmt.Errorf("read failed: %w", io.EOF), true},
		{"wrapped io.ErrUnexpectedEOF", fmt.Errorf("read failed: %w", io.ErrUnexpectedEOF), true},
		{"thereof should not match the EOF pattern", errors.New("the value thereof is invalid"), false},
		{"whereof should not match the EOF pattern", errors.New("the source whereof is unknown"), false},
		{"connection refused", errors.New("dial tcp 127.0.0.1:9000: connection refused"), true},
		{"connection reset", errors.New("read: connection reset by peer"), true},
		{"i/o timeout", errors.New("read tcp: i/o timeout"), true},
		{"no such host", errors.New("dial tcp: lookup ch: no such host"), true},
		{"network unreachable", errors.New("dial tcp: network is unreachable"), true},
		{"broken pipe", errors.New("write: broken pipe"), true},
		{"data validation error is not retryable", errors.New("invalid data type for column"), false},
		{"net.Error", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{"commitError is not retryable", &commitError{err: errors.New("connection lost during commit")}, false},
		{
			"wrapped commitError is not retryable",
			fmt.Errorf("flush failed: %w", &commitError{err: io.EOF}),
			false,
		},
		{
			"commitError inside retry-go's error list is not retryable, regardless of position",
			retry.Error{errors.New("transient blip"), &commitError{err: errors.New("connection lost during commit")}},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, isRetryableError(tt.err))
		})
	}
}

func TestCommitError(t *testing.T) {
	t.Parallel()

	inner := errors.New("timeout")
	ce := &commitError{err: inner}

	assert.Equal(t, "commit error: timeout", ce.Error())
	assert.Equal(t, inner, errors.Unwrap(ce))
}

func TestBound(t *testing.T) {
	t.Parallel()

	newSamples := func(values ...float64) []metrics.Sample {
		samples := make([]metrics.Sample, len(values))
		for i, v := range values {
			samples[i] = metrics.Sample{Value: v}
		}
		return samples
	}

	tests := []struct {
		name        string
		samples     []metrics.Sample
		limit       int
		policy      string
		wantValues  []float64
		wantDropped int
	}{
		{
			name:        "under limit keeps everything, oldest policy",
			samples:     newSamples(1, 2, 3),
			limit:       5,
			policy:      dropOldest,
			wantValues:  []float64{1, 2, 3},
			wantDropped: 0,
		},
		{
			name:        "at limit keeps everything, newest policy",
			samples:     newSamples(1, 2, 3),
			limit:       3,
			policy:      dropNewest,
			wantValues:  []float64{1, 2, 3},
			wantDropped: 0,
		},
		{
			name:        "over limit drops oldest",
			samples:     newSamples(1, 2, 3, 4, 5),
			limit:       3,
			policy:      dropOldest,
			wantValues:  []float64{3, 4, 5},
			wantDropped: 2,
		},
		{
			name:        "over limit drops newest",
			samples:     newSamples(1, 2, 3, 4, 5),
			limit:       3,
			policy:      dropNewest,
			wantValues:  []float64{1, 2, 3},
			wantDropped: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kept, dropped := bound(tt.samples, tt.limit, tt.policy)
			assert.Equal(t, tt.wantDropped, dropped)

			require.Len(t, kept, len(tt.wantValues))
			for i, want := range tt.wantValues {
				assert.Equal(t, want, kept[i].Value)
			}
		})
	}
}

func TestFlush_NoSamplesIsANoOp(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t)
	o.flush()

	assert.Zero(t, o.stats.flushFailures)
	assert.Zero(t, o.stats.written)
}

func TestFlush_FailureBuffersSamples(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t)

	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 1, 2), makeSamples(t, 3)})
	o.flush()

	assert.Len(t, o.pending, 3)
	assert.Equal(t, uint64(1), o.stats.flushFailures)
	assert.Zero(t, o.stats.dropped)
	assert.Zero(t, o.stats.written)
	assert.Zero(t, o.stats.retries, "errNotStarted must not be retried")

	// The next flush retries pending samples first, then the new ones.
	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 4)})
	o.flush()

	require.Len(t, o.pending, 4)
	for i, s := range o.pending {
		assert.Equal(t, float64(i+1), s.Value)
	}
	assert.Equal(t, uint64(2), o.stats.flushFailures)
}

func TestFlush_BufferOverflowDropsSamples(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t, map[string]any{"bufferMaxSamples": 2, "bufferDropPolicy": dropOldest})

	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 1, 2, 3)})
	o.flush()

	require.Len(t, o.pending, 2)
	assert.Equal(t, 2.0, o.pending[0].Value)
	assert.Equal(t, uint64(1), o.stats.dropped)
}

func TestFlush_BufferingDisabledDropsSamples(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t, map[string]any{"bufferEnabled": false})

	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 1, 2, 3)})
	o.flush()

	assert.Empty(t, o.pending)
	assert.Equal(t, uint64(3), o.stats.dropped)
	assert.Equal(t, uint64(1), o.stats.flushFailures)
}

// TestWrite_ConvertsOnce guards that conversion happens once per write, not
// once per retry attempt, and that samples failing conversion are not
// re-buffered.
func TestWrite_ConvertsOnce(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t, map[string]any{
		"retryAttempts": 3,
		"retryDelay":    "1ms",
		"retryMaxDelay": "1ms",
	})
	o.schema = negativeFailsSchema{}
	o.db = refusingDB(t)

	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 1, -1, 2, -2, 3)})
	o.flush()

	assert.Equal(t, uint64(2), o.stats.convertErrors, "convert errors must be counted once, not per attempt")
	assert.Equal(t, uint64(3), o.stats.retries, "retries must not count the final attempt")
	assert.Equal(t, uint64(1), o.stats.flushFailures)
	assert.Zero(t, o.stats.written)

	require.Len(t, o.pending, 3, "only converted samples are re-buffered")
	for i, s := range o.pending {
		assert.Equal(t, float64(i+1), s.Value)
	}
}

func TestWrite_AllConvertErrors(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t)
	o.schema = negativeFailsSchema{}

	kept, err := o.write(t.Context(), makeSamples(t, -1, -2))
	require.NoError(t, err, "nothing to insert is not an insert failure")
	assert.Empty(t, kept)
	assert.Equal(t, uint64(2), o.stats.convertErrors)
}

// TestWrite_RetryCancelledDuringBackoff guards that a retry cut short by ctx
// during the backoff wait is not counted.
func TestWrite_RetryCancelledDuringBackoff(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t, map[string]any{
		"retryAttempts": 3,
		"retryDelay":    "1m",
		"retryMaxDelay": "1m",
	})
	o.db = refusingDB(t)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := o.write(ctx, makeSamples(t, 1))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Zero(t, o.stats.retries)
}

func TestInsertRows_NotStarted(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t)
	err := o.insertRows(t.Context(), [][]any{{1}})
	require.ErrorIs(t, err, errNotStarted)
	assert.False(t, isRetryableError(err))
}

// TestInsertRows_ContextDoneBeforeCommit guards that rows whose context ends
// before Commit are reported as lost, not as an ambiguous commit error
// counted as written: database/sql rolls such a transaction back.
func TestInsertRows_ContextDoneBeforeCommit(t *testing.T) {
	t.Parallel()

	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelled=%v", cancelled), func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fake := &fakeDB{}
			if cancelled {
				fake.cancel = cancel
			}
			o := newTestOutput(t)
			o.db = sql.OpenDB(fake)
			t.Cleanup(func() { _ = o.db.Close() })

			err := o.insertRows(ctx, [][]any{{1}})
			if !cancelled {
				require.NoError(t, err)
				assert.True(t, fake.committed.Load())
				assert.Equal(t, uint64(1), o.stats.written)
				return
			}
			require.ErrorIs(t, err, context.Canceled)
			assert.False(t, isCommitError(err))
			assert.False(t, fake.committed.Load())
			assert.Zero(t, o.stats.written)
		})
	}
}

// TestStop_DrainsPendingAndAccountsLoss guards the shutdown drain path:
// pending must be emptied, and samples that cannot be drained (here, because
// Start never ran) must be counted as dropped rather than silently lost.
func TestStop_DrainsPendingAndAccountsLoss(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t)

	// Simulate samples buffered during a prior outage. db is nil, so the drain
	// has nowhere to write them, exercising the loss accounting.
	o.pending = append(o.pending, makeSamples(t, 1)...)
	o.pending = append(o.pending, makeSamples(t, 1)...)
	require.Len(t, o.pending, 2)

	start := time.Now()
	require.NoError(t, o.Stop())
	assert.Less(t, time.Since(start), time.Second, "errNotStarted must not be retried")

	assert.Empty(t, o.pending, "pending should be emptied by the shutdown drain")
	assert.Equal(t, uint64(2), o.stats.dropped,
		"undrainable pending samples must be counted as dropped on shutdown")
	assert.Zero(t, o.stats.retries)

	// A second Stop is a no-op: it must not drain or count anything again.
	o.pending = append(o.pending, makeSamples(t, 1)...)
	require.NoError(t, o.Stop())
	assert.Len(t, o.pending, 1, "second Stop must not drain")
	assert.Equal(t, uint64(2), o.stats.dropped)
}

func TestOutput_StopWithoutStart(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t)
	assert.NoError(t, o.Stop())
}

// makeSamples builds one sample per value.
func makeSamples(t *testing.T, values ...float64) metrics.Samples {
	t.Helper()
	registry := metrics.NewRegistry()
	metric := registry.MustNewMetric("test_metric", metrics.Counter)
	samples := make(metrics.Samples, len(values))
	for i, v := range values {
		samples[i] = metrics.Sample{
			TimeSeries: metrics.TimeSeries{Metric: metric, Tags: registry.RootTagSet()},
			Time:       time.Now(),
			Value:      v,
		}
	}
	return samples
}

// negativeFailsSchema fails to convert samples with a negative value.
type negativeFailsSchema struct{ simpleSchema }

func (negativeFailsSchema) Row(s metrics.Sample) ([]any, error) {
	if s.Value < 0 {
		return nil, errors.New("negative value")
	}
	return simpleSchema{}.Row(s)
}

// refusingDB returns a database handle whose connections are refused, which
// isRetryableError treats as transient.
func refusingDB(t *testing.T) *sql.DB {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:        []string{addr},
		DialTimeout: time.Second,
	})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// fakeDB is a database/sql driver that accepts any statement. When cancel is
// set, executing a statement calls it, as if a deadline expired between
// sending the rows and committing them.
type fakeDB struct {
	cancel    context.CancelFunc
	committed atomic.Bool
}

func (d *fakeDB) Connect(context.Context) (driver.Conn, error) { return d, nil }
func (d *fakeDB) Driver() driver.Driver                        { return d }
func (d *fakeDB) Open(string) (driver.Conn, error)             { return d, nil }
func (d *fakeDB) Prepare(string) (driver.Stmt, error)          { return d, nil }
func (d *fakeDB) Begin() (driver.Tx, error)                    { return d, nil }
func (d *fakeDB) Close() error                                 { return nil }
func (d *fakeDB) NumInput() int                                { return -1 }
func (d *fakeDB) Query([]driver.Value) (driver.Rows, error)    { return nil, errors.New("unsupported") }
func (d *fakeDB) Rollback() error                              { return nil }
func (d *fakeDB) Commit() error                                { d.committed.Store(true); return nil }
func (d *fakeDB) Exec([]driver.Value) (driver.Result, error) {
	if d.cancel != nil {
		d.cancel()
	}
	return driver.RowsAffected(1), nil
}

// Benchmark tests

func BenchmarkOutput_Description(b *testing.B) {
	out := &clickhouseOutput{
		config: config{
			Addr:         "localhost:9000",
			Database:     "k6",
			Table:        "samples",
			PushInterval: 1 * time.Second,
		},
	}

	for b.Loop() {
		_ = out.Description()
	}
}

func BenchmarkOutput_New(b *testing.B) {
	params := output.Params{
		Logger: newTestLogger(b),
		JSONConfig: mustMarshalJSON(map[string]any{
			"addr":         "localhost:9000",
			"database":     "k6",
			"table":        "samples",
			"pushInterval": "1s",
		}),
	}

	for b.Loop() {
		out, err := New(params)
		if err != nil {
			b.Fatal(err)
		}
		_ = out
	}
}
