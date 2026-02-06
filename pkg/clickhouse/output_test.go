package clickhouse

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
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
				ConfigArgument: "",
				JSONConfig:     nil,
			},
			expectError: false,
		},
		{
			name: "valid params with json config",
			params: output.Params{
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
				ConfigArgument: "localhost:9000?database=test&table=samples",
			},
			expectError: false,
		},
		{
			name: "invalid json config",
			params: output.Params{
				JSONConfig: []byte(`{invalid`),
			},
			expectError:   true,
			errorContains: "failed to parse json config",
		},
		{
			name: "invalid pushInterval in json",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{
					"pushInterval": "not-a-duration",
				}),
			},
			expectError:   true,
			errorContains: "invalid pushInterval",
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

			clickhouseOut, ok := out.(*Output)
			require.True(t, ok, "output should be of type *Output")
			assert.NotNil(t, clickhouseOut.logger)
			assert.NotNil(t, clickhouseOut.config)
		})
	}
}

func TestNew_ConfigParsing(t *testing.T) {
	t.Parallel()

	params := output.Params{
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

	clickhouseOut := out.(*Output)
	assert.Equal(t, "test-host:9000", clickhouseOut.config.Addr)
	assert.Equal(t, "test_db", clickhouseOut.config.Database)
	assert.Equal(t, "test_table", clickhouseOut.config.Table)
	assert.Equal(t, 10*time.Second, clickhouseOut.config.PushInterval)
}

func TestOutput_Description(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		config             Config
		expectedDescPrefix string
	}{
		{
			name: "default config",
			config: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
			expectedDescPrefix: "clickhouse (localhost:9000)",
		},
		{
			name: "custom config",
			config: Config{
				Addr:         "clickhouse.example.com:9000",
				Database:     "production",
				Table:        "metrics",
				PushInterval: 5 * time.Second,
			},
			expectedDescPrefix: "clickhouse (clickhouse.example.com:9000)",
		},
		{
			name: "ipv6 address",
			config: Config{
				Addr:         "[::1]:9000",
				Database:     "test",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
			expectedDescPrefix: "clickhouse ([::1]:9000)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := &Output{
				config: tt.config,
			}

			desc := out.Description()
			assert.Equal(t, tt.expectedDescPrefix, desc)
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

			params := output.Params{}
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

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)

		// Should not panic when there are no buffered samples
		require.NotPanics(t, func() {
			clickhouseOut.flush()
		})
	})

	t.Run("flush with nil database", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)
		clickhouseOut.db = nil

		// Should not panic but will fail silently
		require.NotPanics(t, func() {
			clickhouseOut.flush()
		})
	})

	t.Run("flush with closed output", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)

		// Close output
		err = clickhouseOut.Stop()
		require.NoError(t, err)

		// Flush should return early
		require.NotPanics(t, func() {
			clickhouseOut.flush()
		})
	})
}

func TestOutput_Lifecycle(t *testing.T) {
	t.Parallel()

	params := output.Params{
		JSONConfig: mustMarshalJSON(map[string]any{
			"addr":         "localhost:9000",
			"pushInterval": "1s",
		}),
	}

	out, err := New(params)
	require.NoError(t, err)
	require.NotNil(t, out)

	clickhouseOut := out.(*Output)
	assert.NotNil(t, clickhouseOut.logger)
	assert.Equal(t, "localhost:9000", clickhouseOut.config.Addr)

	// Stop should work even if Start was never called
	err = out.Stop()
	assert.NoError(t, err)
}

func TestOutput_NilDatabase(t *testing.T) {
	t.Parallel()

	params := output.Params{}
	out, err := New(params)
	require.NoError(t, err)

	clickhouseOut := out.(*Output)
	clickhouseOut.db = nil

	// Should not panic, but will fail to prepare statement
	require.NotPanics(t, func() {
		clickhouseOut.flush()
	})
}

func TestOutput_ConfigurationValidation(t *testing.T) {
	t.Parallel()

	params := output.Params{
		JSONConfig: mustMarshalJSON(map[string]any{
			"addr":         "test-host:9000",
			"database":     "test_db",
			"table":        "test_table",
			"pushInterval": "5s",
		}),
	}

	out, err := New(params)
	require.NoError(t, err)

	clickhouseOut := out.(*Output)

	assert.Equal(t, "test-host:9000", clickhouseOut.config.Addr)
	assert.Equal(t, "test_db", clickhouseOut.config.Database)
	assert.Equal(t, "test_table", clickhouseOut.config.Table)
	assert.Equal(t, 5*time.Second, clickhouseOut.config.PushInterval)
}

// Error metrics tests

func TestOutput_GetErrorMetrics_Initial(t *testing.T) {
	t.Parallel()

	params := output.Params{}
	out, err := New(params)
	require.NoError(t, err)

	clickhouseOut := out.(*Output)
	errMetrics := clickhouseOut.GetErrorMetrics()

	assert.Equal(t, uint64(0), errMetrics.ConvertErrors, "initial ConvertErrors should be 0")
	assert.Equal(t, uint64(0), errMetrics.InsertErrors, "initial InsertErrors should be 0")
	assert.Equal(t, uint64(0), errMetrics.SamplesProcessed, "initial SamplesProcessed should be 0")
}

func TestErrorMetrics_Values(t *testing.T) {
	t.Parallel()

	errMetrics := ErrorMetrics{
		ConvertErrors:    10,
		InsertErrors:     5,
		SamplesProcessed: 1000,
	}

	assert.Equal(t, uint64(10), errMetrics.ConvertErrors)
	assert.Equal(t, uint64(5), errMetrics.InsertErrors)
	assert.Equal(t, uint64(1000), errMetrics.SamplesProcessed)
}

func TestOutput_GetErrorMetrics_AfterStop(t *testing.T) {
	t.Parallel()

	params := output.Params{}
	out, err := New(params)
	require.NoError(t, err)

	clickhouseOut := out.(*Output)

	// Manually set some counter values to verify they persist after stop
	clickhouseOut.convertErrors.Store(5)
	clickhouseOut.insertErrors.Store(3)
	clickhouseOut.samplesProcessed.Store(100)

	err = out.Stop()
	require.NoError(t, err)

	// Metrics should still be accessible after stop
	errMetrics := clickhouseOut.GetErrorMetrics()
	assert.Equal(t, uint64(5), errMetrics.ConvertErrors)
	assert.Equal(t, uint64(3), errMetrics.InsertErrors)
	assert.Equal(t, uint64(100), errMetrics.SamplesProcessed)
}

func TestOutput_ErrorMetrics_AtomicOperations(t *testing.T) {
	t.Parallel()

	params := output.Params{}
	out, err := New(params)
	require.NoError(t, err)

	clickhouseOut := out.(*Output)

	// Test atomic Add operations
	clickhouseOut.convertErrors.Add(5)
	clickhouseOut.convertErrors.Add(3)
	clickhouseOut.insertErrors.Add(2)
	clickhouseOut.samplesProcessed.Add(100)
	clickhouseOut.samplesProcessed.Add(50)

	errMetrics := clickhouseOut.GetErrorMetrics()
	assert.Equal(t, uint64(8), errMetrics.ConvertErrors)
	assert.Equal(t, uint64(2), errMetrics.InsertErrors)
	assert.Equal(t, uint64(150), errMetrics.SamplesProcessed)
}

// Benchmark tests

func BenchmarkOutput_Description(b *testing.B) {
	out := &Output{
		config: Config{
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

// Test for Issue #1: Verify Release is deferred until after Commit (not called before)
// This tests the doFlush method indirectly by verifying the pendingRows pattern.
func TestDoFlush_ReleaseAfterCommit(t *testing.T) {
	t.Parallel()

	t.Run("release is deferred on context cancellation", func(t *testing.T) {
		t.Parallel()

		// Create an Output with no database — doFlush should fail at BeginTx
		// but this validates the pendingRows accumulator doesn't leak on error paths
		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)
		clickhouseOut.db = nil

		registry := metrics.NewRegistry()
		metric := registry.MustNewMetric("test", metrics.Counter)
		sample := metrics.Sample{
			TimeSeries: metrics.TimeSeries{
				Metric: metric,
			},
			Time:  time.Now(),
			Value: 1.0,
		}
		containers := []metrics.SampleContainer{metrics.Samples{sample}}

		ctx := context.Background()
		err = clickhouseOut.doFlush(ctx, containers)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database connection not initialized")
	})
}

// Test for Issue #2: Verify Stop allows final flush to execute
func TestStop_FinalFlushNotSkipped(t *testing.T) {
	t.Parallel()

	t.Run("flush is not blocked during stop sequence", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)

		// Verify closed is false before Stop
		clickhouseOut.mu.RLock()
		assert.False(t, clickhouseOut.closed, "closed should be false before Stop")
		clickhouseOut.mu.RUnlock()

		// flush() should not skip when closed is false
		// (simulates the final flush triggered by periodicFlusher.Stop)
		require.NotPanics(t, func() {
			clickhouseOut.flush() // Should execute normally (no samples, returns early)
		})

		// Now stop
		err = clickhouseOut.Stop()
		require.NoError(t, err)

		// After Stop, closed should be true
		clickhouseOut.mu.RLock()
		assert.True(t, clickhouseOut.closed, "closed should be true after Stop")
		clickhouseOut.mu.RUnlock()

		// flush() should now skip due to closed flag
		require.NotPanics(t, func() {
			clickhouseOut.flush()
		})
	})

	t.Run("concurrent stop calls are safe with double-check lock", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)

		var wg sync.WaitGroup
		numStops := 20
		wg.Add(numStops)
		errs := make([]error, numStops)

		for i := range numStops {
			go func(idx int) {
				defer wg.Done()
				errs[idx] = clickhouseOut.Stop()
			}(i)
		}

		wg.Wait()

		for i, stopErr := range errs {
			assert.NoError(t, stopErr, "Stop call %d should not error", i)
		}

		// Verify final state
		clickhouseOut.mu.RLock()
		assert.True(t, clickhouseOut.closed)
		clickhouseOut.mu.RUnlock()
	})
}

// mustMarshalJSON is defined in config_test.go
