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
		name          string
		params        output.Params
		expectError   bool
		errorContains string
	}{
		{
			name: "valid params with defaults",
			params: output.Params{
				Logger:         newTestLogger(t),
				ConfigArgument: "",
				JSONConfig:     nil,
			},
			expectError: false,
		},
		{
			name: "valid params with json config",
			params: output.Params{
				Logger: newTestLogger(t),
				JSONConfig: mustMarshalJSON(map[string]any{
					"addr":         "clickhouse:9000",
					"database":     "metrics",
					"table":        "k6_samples",
					"pushInterval": "5s",
				}),
			},
			expectError: false,
		},
		{
			name: "valid params with url config",
			params: output.Params{
				Logger:         newTestLogger(t),
				ConfigArgument: "localhost:9000?database=test&table=samples",
			},
			expectError: false,
		},
		{
			name: "invalid json config",
			params: output.Params{
				Logger:     newTestLogger(t),
				JSONConfig: []byte(`{invalid`),
			},
			expectError:   true,
			errorContains: "json config: invalid character",
		},
		{
			name: "invalid pushInterval in json",
			params: output.Params{
				Logger: newTestLogger(t),
				JSONConfig: mustMarshalJSON(map[string]any{
					"pushInterval": "not-a-duration",
				}),
			},
			expectError:   true,
			errorContains: "invalid pushInterval",
		},
		{
			name: "unknown schema mode",
			params: output.Params{
				Logger: newTestLogger(t),
				JSONConfig: mustMarshalJSON(map[string]any{
					"schemaMode": "nope",
				}),
			},
			expectError:   true,
			errorContains: `unknown schemaMode "nope"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := New(tt.params)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, out)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)

			clickhouseOut, ok := out.(*clickhouseOutput)
			require.True(t, ok, "output should be of type *clickhouseOutput")
			assert.NotNil(t, clickhouseOut.logger)
			assert.NotNil(t, clickhouseOut.config)
		})
	}
}

func TestNew_ConfigParsing(t *testing.T) {
	t.Parallel()

	params := output.Params{
		Logger: newTestLogger(t),
		JSONConfig: mustMarshalJSON(map[string]any{
			"addr":         "test-host:9000",
			"database":     "test_db",
			"table":        "test_table",
			"pushInterval": "10s",
		}),
	}

	out, err := New(params)
	require.NoError(t, err)
	require.NotNil(t, out)

	clickhouseOut := out.(*clickhouseOutput)
	assert.Equal(t, "test-host:9000", clickhouseOut.config.Addr)
	assert.Equal(t, "test_db", clickhouseOut.config.Database)
	assert.Equal(t, "test_table", clickhouseOut.config.Table)
	assert.Equal(t, 10*time.Second, clickhouseOut.config.PushInterval)
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

func TestOutput_Stop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		description string
	}{
		{
			name:        "stop with nil db and periodicFlusher",
			description: "should not panic",
		},
		{
			name:        "stop without start",
			description: "stop can be called without start",
		},
		{
			name:        "stop is idempotent",
			description: "multiple stop calls should succeed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			params := output.Params{Logger: newTestLogger(t)}
			out, err := New(params)
			require.NoError(t, err)

			// First stop
			err = out.Stop()
			assert.NoError(t, err)

			// Second stop (tests idempotency)
			err = out.Stop()
			assert.NoError(t, err)
		})
	}
}

func TestOutput_Flush(t *testing.T) {
	t.Parallel()

	t.Run("flush with no samples", func(t *testing.T) {
		t.Parallel()

		params := output.Params{Logger: newTestLogger(t)}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*clickhouseOutput)

		// Should not panic when there are no buffered samples
		require.NotPanics(t, func() {
			clickhouseOut.flush()
		})
	})

	t.Run("flush with nil database", func(t *testing.T) {
		t.Parallel()

		params := output.Params{Logger: newTestLogger(t)}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*clickhouseOutput)
		clickhouseOut.db = nil

		// Should not panic but will fail silently
		require.NotPanics(t, func() {
			clickhouseOut.flush()
		})
	})
}

func TestOutput_Lifecycle(t *testing.T) {
	t.Parallel()

	params := output.Params{
		Logger: newTestLogger(t),
		JSONConfig: mustMarshalJSON(map[string]any{
			"addr":         "localhost:9000",
			"pushInterval": "1s",
		}),
	}

	out, err := New(params)
	require.NoError(t, err)
	require.NotNil(t, out)

	clickhouseOut := out.(*clickhouseOutput)
	assert.NotNil(t, clickhouseOut.logger)
	assert.Equal(t, "localhost:9000", clickhouseOut.config.Addr)

	// Stop should work even if Start was never called
	err = out.Stop()
	assert.NoError(t, err)
}

func TestOutput_ConfigurationValidation(t *testing.T) {
	t.Parallel()

	clickhouseOut := newTestOutput(t, map[string]any{
		"addr":         "test-host:9000",
		"database":     "test_db",
		"table":        "test_table",
		"pushInterval": "5s",
	})

	assert.NotNil(t, clickhouseOut.logger)
	assert.Equal(t, "test-host:9000", clickhouseOut.config.Addr)
	assert.Equal(t, "test_db", clickhouseOut.config.Database)
	assert.Equal(t, "test_table", clickhouseOut.config.Table)
	assert.Equal(t, 5*time.Second, clickhouseOut.config.PushInterval)

	// Stop should work even if Start was never called
	err := clickhouseOut.Stop()
	assert.NoError(t, err)
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

// Test for Issue #4: isRetryableError uses typed EOF checks instead of broad "eof" pattern
func TestIsRetryableError_EOFPatternFix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "io.EOF",
			err:      io.EOF,
			expected: true,
		},
		{
			name:     "io.ErrUnexpectedEOF",
			err:      io.ErrUnexpectedEOF,
			expected: true,
		},
		{
			name:     "wrapped io.EOF",
			err:      fmt.Errorf("read failed: %w", io.EOF),
			expected: true,
		},
		{
			name:     "wrapped io.ErrUnexpectedEOF",
			err:      fmt.Errorf("read failed: %w", io.ErrUnexpectedEOF),
			expected: true,
		},
		{
			name:     "thereof should not match",
			err:      errors.New("the value thereof is invalid"),
			expected: false,
		},
		{
			name:     "whereof should not match",
			err:      errors.New("the source whereof is unknown"),
			expected: false,
		},
		{
			name:     "connection refused",
			err:      errors.New("dial tcp 127.0.0.1:9000: connection refused"),
			expected: true,
		},
		{
			name:     "data validation error not retryable",
			err:      errors.New("invalid data type for column"),
			expected: false,
		},
		{
			name:     "network error",
			err:      &net.OpError{Op: "dial", Err: errors.New("connection refused")},
			expected: true,
		},
		{
			name:     "broken pipe",
			err:      errors.New("write: broken pipe"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test for Issue #7: commitError is not retryable
func TestIsRetryableError_CommitError(t *testing.T) {
	t.Parallel()

	t.Run("commitError is not retryable", func(t *testing.T) {
		t.Parallel()
		ce := &commitError{err: errors.New("connection lost during commit")}
		assert.False(t, isRetryableError(ce))
	})

	t.Run("wrapped commitError is not retryable", func(t *testing.T) {
		t.Parallel()
		ce := &commitError{err: io.EOF}
		wrapped := fmt.Errorf("flush failed: %w", ce)
		assert.False(t, isRetryableError(wrapped))
	})

	t.Run("commitError Unwrap returns inner error", func(t *testing.T) {
		t.Parallel()
		inner := errors.New("timeout")
		ce := &commitError{err: inner}
		assert.Equal(t, inner, errors.Unwrap(ce))
	})

	t.Run("commitError Error message", func(t *testing.T) {
		t.Parallel()
		ce := &commitError{err: errors.New("some db error")}
		assert.Contains(t, ce.Error(), "commit error: some db error")
	})
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

// TestInsertRows_ContextDoneBeforeCommit guards that rows whose context ends
// before Commit are reported as lost, not as an ambiguous commit error counted
// as written: database/sql rolls such a transaction back.
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

func TestNew_UsesParamsLogger(t *testing.T) {
	t.Parallel()
	l := logrus.New()
	l.SetOutput(io.Discard)
	params := output.Params{
		Logger: l,
	}
	out, err := New(params)
	require.NoError(t, err)
	assert.NotNil(t, out.(*clickhouseOutput).logger)
}

func TestNew_FallbackLogger(t *testing.T) {
	t.Parallel()
	params := output.Params{}
	out, err := New(params)
	require.NoError(t, err)
	assert.NotNil(t, out.(*clickhouseOutput).logger)
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

// mustMarshalJSON is defined in config_test.go
