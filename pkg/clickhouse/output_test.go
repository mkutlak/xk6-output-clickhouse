package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	metrics := clickhouseOut.GetErrorMetrics()

	assert.Equal(t, uint64(0), metrics.ConvertErrors, "initial ConvertErrors should be 0")
	assert.Equal(t, uint64(0), metrics.InsertErrors, "initial InsertErrors should be 0")
	assert.Equal(t, uint64(0), metrics.SamplesProcessed, "initial SamplesProcessed should be 0")
}

func TestErrorMetrics_Values(t *testing.T) {
	t.Parallel()

	metrics := ErrorMetrics{
		ConvertErrors:    10,
		InsertErrors:     5,
		SamplesProcessed: 1000,
	}

	assert.Equal(t, uint64(10), metrics.ConvertErrors)
	assert.Equal(t, uint64(5), metrics.InsertErrors)
	assert.Equal(t, uint64(1000), metrics.SamplesProcessed)
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
	metrics := clickhouseOut.GetErrorMetrics()
	assert.Equal(t, uint64(5), metrics.ConvertErrors)
	assert.Equal(t, uint64(3), metrics.InsertErrors)
	assert.Equal(t, uint64(100), metrics.SamplesProcessed)
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

	metrics := clickhouseOut.GetErrorMetrics()
	assert.Equal(t, uint64(8), metrics.ConvertErrors)
	assert.Equal(t, uint64(2), metrics.InsertErrors)
	assert.Equal(t, uint64(150), metrics.SamplesProcessed)
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

// mustMarshalJSON is defined in config_test.go
