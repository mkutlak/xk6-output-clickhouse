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

	t.Run("config is properly parsed", func(t *testing.T) {
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
	})
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

func TestOutput_Start(t *testing.T) {
	t.Parallel()

	t.Run("start requires real database connection", func(t *testing.T) {
		t.Parallel()

		// Skip this test as it requires a real ClickHouse instance
		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// params := output.Params{
		//     ConfigArgument: "localhost:9000",
		// }
		// out, err := New(params)
		// require.NoError(t, err)
		//
		// err = out.Start()
		// require.NoError(t, err)
		// assert.NotNil(t, out.(*Output).db)
		// assert.NotNil(t, out.(*Output).periodicFlusher)
		//
		// out.Stop()
	})
}

func TestOutput_Start_ConnectionFailure(t *testing.T) {
	t.Parallel()

	t.Run("start fails with invalid address", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires attempting real connection")

		// Expected behavior:
		// params := output.Params{
		//     ConfigArgument: "invalid-host:9999",
		// }
		// out, err := New(params)
		// require.NoError(t, err)
		//
		// err = out.Start()
		// require.Error(t, err)
		// assert.Contains(t, err.Error(), "failed to ping ClickHouse")
	})
}

func TestOutput_Stop(t *testing.T) {
	t.Parallel()

	t.Run("stop with nil db and periodicFlusher", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		// Should not panic
		err = out.Stop()
		assert.NoError(t, err)
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		err = out.Stop()
		assert.NoError(t, err)

		// Second call should also succeed
		err = out.Stop()
		assert.NoError(t, err)
	})
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
		clickhouseOut.flush()
	})

	t.Run("flush requires database connection", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// out := setupOutputWithConnection(t)
		// defer out.Stop()
		//
		// Add samples to buffer
		// out.AddMetricSamples(createTestSamples())
		//
		// out.flush()
		//
		// Verify samples were written to ClickHouse
	})
}

func TestOutput_Lifecycle(t *testing.T) {
	t.Parallel()

	t.Run("complete lifecycle without database", func(t *testing.T) {
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
	})
}

func TestOutput_Integration(t *testing.T) {
	t.Parallel()

	t.Run("full integration test", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected complete integration test:
		// 1. Create output with test configuration
		// 2. Start the output (connects to ClickHouse, creates schema)
		// 3. Add metric samples to buffer
		// 4. Manually trigger flush or wait for periodic flush
		// 5. Query ClickHouse to verify data was written
		// 6. Stop the output
		// 7. Cleanup test database/table
	})
}

func TestOutput_FlushWithSamples(t *testing.T) {
	t.Parallel()

	t.Run("flush batch insert logic", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Create test samples with various metrics and tags
		// samples := []metrics.SampleContainer{
		//     createSampleContainer("http_req_duration", 123.45, map[string]string{"method": "GET"}),
		//     createSampleContainer("http_reqs", 1, map[string]string{"status": "200"}),
		// }
		//
		// Add to output buffer
		// Flush
		// Verify both samples were inserted correctly
	})
}

func TestOutput_FlushErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("flush handles database errors gracefully", func(t *testing.T) {
		t.Parallel()

		t.Skip("Requires mock database or integration test")

		// Expected behavior:
		// Setup output with failing database connection
		// Add samples to buffer
		// Call flush
		// Verify error is logged but doesn't panic
		// Verify samples remain in buffer for retry
	})
}

func TestOutput_TagsHandling(t *testing.T) {
	t.Parallel()

	t.Run("samples with nil tags", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Create sample with nil tags
		// Flush to ClickHouse
		// Verify tags field is empty map (not null)
	})

	t.Run("samples with empty tags", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Create sample with empty tags map
		// Flush to ClickHouse
		// Verify tags field is empty map
	})

	t.Run("samples with multiple tags", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Create sample with multiple tags
		// tags := map[string]string{
		//     "method": "GET",
		//     "status": "200",
		//     "endpoint": "/api/users",
		// }
		// Flush to ClickHouse
		// Verify all tags are stored correctly
	})
}

func TestOutput_MetricTypes(t *testing.T) {
	t.Parallel()

	t.Run("handles different metric types", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Test different k6 metric types:
		// - Counter (http_reqs)
		// - Gauge (vus)
		// - Rate (http_req_failed)
		// - Trend (http_req_duration)
		//
		// All should be stored as Float64 in value column
	})
}

func TestOutput_ConcurrentFlush(t *testing.T) {
	t.Parallel()

	t.Run("concurrent flush operations", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Test that periodic flusher and manual flush don't interfere
		// Add samples continuously
		// Let periodic flusher run
		// Manually trigger flush
		// Verify no data loss or corruption
	})
}

func TestOutput_PeriodicFlusher(t *testing.T) {
	t.Parallel()

	t.Run("periodic flusher configuration", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Configure short push interval (e.g., 100ms)
		// Start output
		// Add samples
		// Wait for multiple flush intervals
		// Verify samples were flushed periodically
		// Stop output
	})
}

func TestOutput_LargeDataset(t *testing.T) {
	t.Parallel()

	t.Run("flush large number of samples", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Create 10000+ samples
		// Add to buffer
		// Flush
		// Verify all samples were inserted
		// Check performance metrics
	})
}

func TestOutput_TimestampPrecision(t *testing.T) {
	t.Parallel()

	t.Run("preserves millisecond precision", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Create sample with specific timestamp including milliseconds
		// Flush to ClickHouse (DateTime64(3))
		// Query back and verify millisecond precision is preserved
	})
}

func TestOutput_SchemaCreation(t *testing.T) {
	t.Parallel()

	t.Run("creates database and table on start", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Drop database if exists
		// Start output
		// Verify database was created
		// Verify table was created with correct schema
	})
}

func TestOutput_DatabaseConnectionPool(t *testing.T) {
	t.Parallel()

	t.Run("uses connection pooling", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Start output
		// Verify sql.DB is created (which has built-in connection pooling)
		// Multiple flush operations should reuse connections
	})
}

func TestOutput_GracefulShutdown(t *testing.T) {
	t.Parallel()

	t.Run("flushes remaining samples on stop", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Start output
		// Add samples
		// Call Stop
		// Verify periodicFlusher.Stop() was called
		// Verify database connection was closed
		// Verify no samples were lost
	})
}

func TestOutput_ErrorRecovery(t *testing.T) {
	t.Parallel()

	t.Run("recovers from transient errors", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Start output
		// Simulate network issue
		// Add samples
		// Restore network
		// Verify samples are eventually flushed when connection recovers
	})
}

func TestOutput_BatchTransactions(t *testing.T) {
	t.Parallel()

	t.Run("uses transactions for batch inserts", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Add multiple samples
		// Flush
		// Verify all samples are inserted in a single transaction
		// If any sample fails, entire batch should rollback
	})
}

func TestOutput_MetricValidation(t *testing.T) {
	t.Parallel()

	t.Run("handles various metric name formats", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Test metric names with:
		// - Underscores: http_req_duration
		// - Dots: my.custom.metric
		// - Hyphens: my-custom-metric
		// - Numbers: metric_123
		// All should be stored correctly as LowCardinality(String)
	})
}

func TestOutput_ValuePrecision(t *testing.T) {
	t.Parallel()

	t.Run("preserves float64 precision", func(t *testing.T) {
		t.Parallel()

		t.Skip("Integration test - requires real ClickHouse instance")

		// Expected behavior:
		// Create samples with high-precision float values
		// values := []float64{0.123456789, 999999.999999, 0.000001}
		// Flush and query back
		// Verify precision is preserved
	})
}

func TestOutput_NilDatabase(t *testing.T) {
	t.Parallel()

	t.Run("flush with nil database doesn't panic", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)
		clickhouseOut.db = nil

		// Should not panic, but will fail to prepare statement
		// Error will be logged but not returned
		require.NotPanics(t, func() {
			clickhouseOut.flush()
		})
	})
}

func TestOutput_ConfigurationValidation(t *testing.T) {
	t.Parallel()

	t.Run("output stores config correctly", func(t *testing.T) {
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
	})
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

func TestOutput_StopWithoutStart(t *testing.T) {
	t.Parallel()

	t.Run("stop can be called without start", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		// Stop should not panic even if Start was never called
		err = out.Stop()
		assert.NoError(t, err)
	})
}

func TestOutput_DoubleStop(t *testing.T) {
	t.Parallel()

	t.Run("stop can be called multiple times", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		err = out.Stop()
		assert.NoError(t, err)

		// Second stop should also work
		err = out.Stop()
		assert.NoError(t, err)
	})
}

func TestOutput_FlushEmptyBuffer(t *testing.T) {
	t.Parallel()

	t.Run("flush with empty buffer returns early", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)

		// Should return early without attempting database operations
		require.NotPanics(t, func() {
			clickhouseOut.flush()
		})
	})
}
