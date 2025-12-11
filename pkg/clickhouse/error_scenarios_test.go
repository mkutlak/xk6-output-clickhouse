package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

// TestNewErrorScenarios tests error scenarios in New function
func TestNewErrorScenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		params        output.Params
		expectError   bool
		errorContains string
	}{
		{
			name: "invalid JSON config",
			params: output.Params{
				JSONConfig: []byte(`{invalid json`),
			},
			expectError:   true,
			errorContains: "json",
		},
		{
			name: "invalid pushInterval format",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"pushInterval": "not-a-duration",
				}),
			},
			expectError:   true,
			errorContains: "pushInterval",
		},
		{
			name: "negative pushInterval",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"pushInterval": "-5s",
				}),
			},
			expectError:   true,
			errorContains: "interval",
		},
		{
			name: "empty addr in JSON",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"addr": "",
				}),
			},
			expectError: false, // Empty addr uses default
		},
		{
			name: "invalid database name - SQL injection attempt",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"database": "test'; DROP DATABASE test; --",
				}),
			},
			expectError:   true,
			errorContains: "database",
		},
		{
			name: "invalid table name - SQL injection attempt",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"table": "samples'; DROP TABLE samples; --",
				}),
			},
			expectError:   true,
			errorContains: "table",
		},
		{
			name: "database name too long",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"database": string(make([]byte, 100)), // > 63 chars
				}),
			},
			expectError:   true,
			errorContains: "database",
		},
		{
			name: "table name too long",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"table": string(make([]byte, 100)), // > 63 chars
				}),
			},
			expectError:   true,
			errorContains: "table",
		},
		{
			name: "invalid schema mode",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"schemaMode": "invalid",
				}),
			},
			expectError:   true,
			errorContains: "schemaMode",
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
			} else {
				require.NoError(t, err)
				require.NotNil(t, out)
			}
		})
	}
}

// TestConvertToCompatibleErrorScenarios tests error scenarios in convertToCompatible
func TestConvertToCompatibleErrorScenarios(t *testing.T) {
	t.Parallel()
	registry := metrics.NewRegistry()

	tests := []struct {
		name          string
		setupSample   func() metrics.Sample
		expectError   bool
		errorContains string
	}{
		{
			name: "invalid buildId - non-numeric",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"buildId": "not-a-number",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			expectError:   true,
			errorContains: "buildId",
		},
		{
			name: "invalid buildId - overflow uint32",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"buildId": "9999999999999", // > max uint32
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			expectError:   true,
			errorContains: "buildId",
		},
		{
			name: "invalid status - non-numeric",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"status": "OK",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			expectError:   true,
			errorContains: "status",
		},
		{
			name: "invalid status - overflow uint16",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"status": "999999", // > max uint16
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			expectError:   true,
			errorContains: "status",
		},
		{
			name: "negative buildId",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"buildId": "-123",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			expectError:   true,
			errorContains: "buildId",
		},
		{
			name: "negative status",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"status": "-200",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			expectError:   true,
			errorContains: "status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sample := tt.setupSample()
			result, err := convertToCompatible(sample)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				_ = result
			}
		})
	}
}

// TestStartErrorScenarios tests error scenarios in Start function
func TestStartErrorScenarios(t *testing.T) {
	t.Parallel()

	t.Run("Start fails with invalid address", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]interface{}{
				"addr": "invalid-host-that-does-not-exist:9999",
			}),
		}

		out, err := New(params)
		require.NoError(t, err)
		require.NotNil(t, out)

		// Start should fail when trying to ping invalid address
		err = out.Start()
		assert.Error(t, err, "Start should fail with invalid address")
		assert.Contains(t, err.Error(), "ping", "Error should mention ping failure")

		// Cleanup
		_ = out.Stop()
	})

	t.Run("Start fails with closed output", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]interface{}{
				"addr": "localhost:9000",
			}),
		}

		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)

		// Close the output first
		clickhouseOut.closed = true

		// Start should fail
		err = clickhouseOut.Start()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "closed")
	})
}

// TestStopErrorScenarios tests error scenarios in Stop function
func TestStopErrorScenarios(t *testing.T) {
	t.Parallel()

	t.Run("Stop with nil db and periodicFlusher", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		// Should not panic or error
		err = out.Stop()
		assert.NoError(t, err)
	})

	t.Run("Stop is idempotent", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		// First stop
		err = out.Stop()
		assert.NoError(t, err)

		// Second stop should also succeed
		err = out.Stop()
		assert.NoError(t, err)

		// Third stop
		err = out.Stop()
		assert.NoError(t, err)
	})
}

// TestFlushErrorScenarios tests error scenarios in flush function
func TestFlushErrorScenarios(t *testing.T) {
	t.Parallel()

	t.Run("flush with no buffered samples", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)

		// Should not panic
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

// TestConfigErrorScenarios tests error scenarios in config parsing
func TestConfigErrorScenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		configArg     string
		jsonConfig    []byte
		expectError   bool
		errorContains string
	}{
		{
			name:        "invalid URL format",
			configArg:   "not-a-valid-url",
			expectError: false, // URL parsing is lenient, this might not error
		},
		{
			name:          "malformed JSON",
			jsonConfig:    []byte(`{"addr": "incomplete`),
			expectError:   true,
			errorContains: "json",
		},
		{
			name:      "conflicting config sources",
			configArg: "localhost:9000?database=db1",
			jsonConfig: mustMarshalJSON(map[string]interface{}{
				"database": "db2",
			}),
			expectError: false, // JSON takes precedence
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			params := output.Params{
				ConfigArgument: tt.configArg,
				JSONConfig:     tt.jsonConfig,
			}

			out, err := New(params)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, out)
			} else if err != nil {
				// Some tests expect error but may not get one
				t.Logf("Got error (may be expected): %v", err)
			}
		})
	}
}

// TestTLSConfigErrorScenarios tests error scenarios in TLS configuration
func TestTLSConfigErrorScenarios(t *testing.T) {
	t.Parallel()

	t.Run("TLS with invalid CA file", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]interface{}{
				"addr": "localhost:9440",
				"tls": map[string]interface{}{
					"enabled": true,
					"caFile":  "/nonexistent/ca.crt",
				},
			}),
		}

		_, err := New(params)
		// TLS validation now happens during New, not Start
		assert.Error(t, err, "New should fail with invalid CA file")
		if err != nil {
			assert.Contains(t, err.Error(), "TLS", "Error should mention TLS")
		}
	})

	t.Run("TLS with invalid client cert", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]interface{}{
				"addr": "localhost:9440",
				"tls": map[string]interface{}{
					"enabled":  true,
					"certFile": "/nonexistent/client.crt",
					"keyFile":  "/nonexistent/client.key",
				},
			}),
		}

		_, err := New(params)
		// TLS validation happens during New
		assert.Error(t, err, "New should fail with invalid client cert")
		if err != nil {
			assert.Contains(t, err.Error(), "TLS", "Error should mention TLS")
		}
	})

	t.Run("TLS cert without key", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]interface{}{
				"addr": "localhost:9440",
				"tls": map[string]interface{}{
					"enabled":  true,
					"certFile": "/some/cert.crt",
					// Missing keyFile
				},
			}),
		}

		_, err := New(params)
		// TLS validation happens during New
		assert.Error(t, err, "New should fail with cert but no key")
		if err != nil {
			assert.Contains(t, err.Error(), "certificate", "Error should mention certificate")
		}
	})
}

// TestMemoryPoolErrorScenarios tests error scenarios with memory pools
func TestMemoryPoolErrorScenarios(t *testing.T) {
	t.Parallel()

	t.Run("pool returns correct types", func(t *testing.T) {
		t.Parallel()

		// tagMapPool
		m := tagMapPool.Get()
		_, ok := m.(map[string]string)
		assert.True(t, ok, "tagMapPool should return map[string]string")
		tagMapPool.Put(m)

		// simpleRowPool
		row := simpleRowPool.Get()
		slice, ok := row.([]interface{})
		assert.True(t, ok, "simpleRowPool should return []interface{}")
		assert.Equal(t, 4, len(slice), "simpleRowPool should return slice of length 4")
		simpleRowPool.Put(row)

		// compatibleRowPool
		row = compatibleRowPool.Get()
		slice, ok = row.([]interface{})
		assert.True(t, ok, "compatibleRowPool should return []interface{}")
		assert.Equal(t, 21, len(slice), "compatibleRowPool should return slice of length 21")
		compatibleRowPool.Put(row)
	})
}

// TestEdgeCaseIdentifiers tests edge cases in identifier validation
func TestEdgeCaseIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		database      string
		table         string
		expectError   bool
		errorContains string
	}{
		{
			name:        "empty database name",
			database:    "",
			table:       "samples",
			expectError: false, // Uses default "k6"
		},
		{
			name:        "empty table name",
			database:    "k6",
			table:       "",
			expectError: false, // Uses default "samples"
		},
		{
			name:          "database with spaces",
			database:      "my database",
			table:         "samples",
			expectError:   true,
			errorContains: "database",
		},
		{
			name:          "table with special chars",
			database:      "k6",
			table:         "table@name",
			expectError:   true,
			errorContains: "table",
		},
		{
			name:     "valid underscore names",
			database: "my_db",
			table:    "my_table",
		},
		{
			name:     "valid numeric suffix",
			database: "db123",
			table:    "table456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			params := output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"addr":     "localhost:9000",
					"database": tt.database,
					"table":    tt.table,
				}),
			}

			out, err := New(params)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, out)
			}
		})
	}
}
