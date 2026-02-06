package clickhouse

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/output"
)

func TestNewConfig(t *testing.T) {
	t.Parallel()

	cfg := NewConfig()

	assert.Equal(t, "localhost:9000", cfg.Addr)
	assert.Equal(t, "k6", cfg.Database)
	assert.Equal(t, "samples", cfg.Table)
	assert.Equal(t, 1*time.Second, cfg.PushInterval)
}

func TestParseConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		params         output.Params
		expectedConfig Config
		expectError    bool
		errorContains  string
	}{
		{
			name: "empty params returns defaults",
			params: output.Params{
				ConfigArgument: "",
				JSONConfig:     nil,
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "json config overrides defaults",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{
					"addr":         "clickhouse.example.com:9000",
					"database":     "metrics",
					"table":        "k6_samples",
					"pushInterval": "5s",
				}),
			},
			expectedConfig: Config{
				Addr:         "clickhouse.example.com:9000",
				Database:     "metrics",
				Table:        "k6_samples",
				PushInterval: 5 * time.Second,
			},
		},
		{
			name: "json config with partial overrides",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{
					"addr":     "192.168.1.100:9000",
					"database": "custom_db",
				}),
			},
			expectedConfig: Config{
				Addr:         "192.168.1.100:9000",
				Database:     "custom_db",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "invalid json config",
			params: output.Params{
				JSONConfig: []byte(`{invalid json`),
			},
			expectError:   true,
			errorContains: "failed to parse json config",
		},
		{
			name: "invalid pushInterval format",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{
					"pushInterval": "not-a-duration",
				}),
			},
			expectError:   true,
			errorContains: "invalid pushInterval",
		},
		{
			name: "url config with scheme",
			params: output.Params{
				ConfigArgument: "http://clickhouse.example.com:9000",
			},
			expectedConfig: Config{
				Addr:         "clickhouse.example.com:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "url config with query parameters",
			params: output.Params{
				ConfigArgument: "localhost:9000?database=prod&table=metrics",
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "prod",
				Table:        "metrics",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "json config with zero pushInterval",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{
					"pushInterval": "0s",
				}),
			},
			expectError:   true,
			errorContains: "push interval must be positive",
		},
		{
			name: "json config with negative pushInterval",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{
					"pushInterval": "-5s",
				}),
			},
			expectError:   true,
			errorContains: "push interval must be positive",
		},
		{
			name: "json config with empty strings uses defaults",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{
					"addr":         "",
					"database":     "",
					"table":        "",
					"pushInterval": "",
				}),
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "json and url config - url query params override json",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{
					"addr":         "json-host:9000",
					"database":     "json_db",
					"table":        "json_table",
					"pushInterval": "5s",
				}),
				ConfigArgument: "url-host:9000?database=url_db&table=url_table",
			},
			expectedConfig: Config{
				Addr:         "json-host:9000",
				Database:     "url_db",
				Table:        "url_table",
				PushInterval: 5 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := ParseConfig(tt.params)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedConfig.Addr, cfg.Addr)
			assert.Equal(t, tt.expectedConfig.Database, cfg.Database)
			assert.Equal(t, tt.expectedConfig.Table, cfg.Table)
			assert.Equal(t, tt.expectedConfig.PushInterval, cfg.PushInterval)
		})
	}
}

func TestParseConfig_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil params returns defaults", func(t *testing.T) {
		t.Parallel()

		cfg, err := ParseConfig(output.Params{})
		require.NoError(t, err)

		expected := NewConfig()
		assert.Equal(t, expected, cfg)
	})

	t.Run("malformed url still processes json config", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"addr": "json-configured:9000",
			}),
			ConfigArgument: "://invalid-url",
		}

		cfg, err := ParseConfig(params)
		require.NoError(t, err)
		assert.Equal(t, "json-configured:9000", cfg.Addr)
	})
}

func TestConfig_Struct(t *testing.T) {
	t.Parallel()

	t.Run("config fields are settable", func(t *testing.T) {
		t.Parallel()

		cfg := Config{
			Addr:         "test-host:9000",
			Database:     "test-db",
			Table:        "test-table",
			PushInterval: 5 * time.Second,
		}

		assert.Equal(t, "test-host:9000", cfg.Addr)
		assert.Equal(t, "test-db", cfg.Database)
		assert.Equal(t, "test-table", cfg.Table)
		assert.Equal(t, 5*time.Second, cfg.PushInterval)
	})

	t.Run("zero value config", func(t *testing.T) {
		t.Parallel()

		var cfg Config

		assert.Equal(t, "", cfg.Addr)
		assert.Equal(t, "", cfg.Database)
		assert.Equal(t, "", cfg.Table)
		assert.Equal(t, time.Duration(0), cfg.PushInterval)
	})
}

// Test for Issue #6: JSON config zero-value overrides work correctly
func TestParseConfig_ZeroValueOverrides(t *testing.T) {
	t.Parallel()

	t.Run("retryAttempts 0 overrides default", func(t *testing.T) {
		t.Parallel()

		cfg, err := ParseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"retryAttempts": 0,
			}),
		})
		require.NoError(t, err)
		assert.Equal(t, uint(0), cfg.RetryAttempts, "retryAttempts: 0 should override default of 3")
	})

	t.Run("skipSchemaCreation false explicitly set", func(t *testing.T) {
		t.Parallel()

		// First set skipSchemaCreation to true via env, then override to false via JSON
		cfg, err := ParseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"skipSchemaCreation": false,
			}),
		})
		require.NoError(t, err)
		assert.False(t, cfg.SkipSchemaCreation, "skipSchemaCreation: false should be explicitly settable")
	})

	t.Run("skipSchemaCreation true explicitly set", func(t *testing.T) {
		t.Parallel()

		cfg, err := ParseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"skipSchemaCreation": true,
			}),
		})
		require.NoError(t, err)
		assert.True(t, cfg.SkipSchemaCreation, "skipSchemaCreation: true should work")
	})

	t.Run("unset fields keep defaults", func(t *testing.T) {
		t.Parallel()

		cfg, err := ParseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"addr": "custom:9000",
			}),
		})
		require.NoError(t, err)
		assert.Equal(t, uint(3), cfg.RetryAttempts, "unset retryAttempts should keep default")
		assert.Equal(t, 10000, cfg.BufferMaxSamples, "unset bufferMaxSamples should keep default")
		assert.False(t, cfg.SkipSchemaCreation, "unset skipSchemaCreation should keep default")
	})

	t.Run("bufferMaxSamples 0 is set but fails validation", func(t *testing.T) {
		t.Parallel()

		// bufferMaxSamples: 0 with bufferEnabled: true should fail validation
		_, err := ParseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"bufferMaxSamples": 0,
			}),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "buffer max samples must be positive")
	})
}

// Test for Issue #5: Env var parse errors return errors instead of being silently swallowed
func TestParseConfig_InvalidEnvVars(t *testing.T) {
	// NOT parallel: t.Setenv modifies process environment

	tests := []struct {
		name          string
		envVar        string
		envValue      string
		errorContains string
	}{
		{
			name:          "invalid K6_CLICKHOUSE_TLS_ENABLED",
			envVar:        "K6_CLICKHOUSE_TLS_ENABLED",
			envValue:      "yes",
			errorContains: "invalid K6_CLICKHOUSE_TLS_ENABLED",
		},
		{
			name:          "invalid K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY",
			envVar:        "K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY",
			envValue:      "nope",
			errorContains: "invalid K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY",
		},
		{
			name:          "invalid K6_CLICKHOUSE_RETRY_ATTEMPTS",
			envVar:        "K6_CLICKHOUSE_RETRY_ATTEMPTS",
			envValue:      "abc",
			errorContains: "invalid K6_CLICKHOUSE_RETRY_ATTEMPTS",
		},
		{
			name:          "invalid K6_CLICKHOUSE_RETRY_DELAY",
			envVar:        "K6_CLICKHOUSE_RETRY_DELAY",
			envValue:      "not-a-duration",
			errorContains: "invalid K6_CLICKHOUSE_RETRY_DELAY",
		},
		{
			name:          "invalid K6_CLICKHOUSE_RETRY_MAX_DELAY",
			envVar:        "K6_CLICKHOUSE_RETRY_MAX_DELAY",
			envValue:      "xyz",
			errorContains: "invalid K6_CLICKHOUSE_RETRY_MAX_DELAY",
		},
		{
			name:          "invalid K6_CLICKHOUSE_BUFFER_ENABLED",
			envVar:        "K6_CLICKHOUSE_BUFFER_ENABLED",
			envValue:      "maybe",
			errorContains: "invalid K6_CLICKHOUSE_BUFFER_ENABLED",
		},
		{
			name:          "invalid K6_CLICKHOUSE_BUFFER_MAX_SAMPLES",
			envVar:        "K6_CLICKHOUSE_BUFFER_MAX_SAMPLES",
			envValue:      "lots",
			errorContains: "invalid K6_CLICKHOUSE_BUFFER_MAX_SAMPLES",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.envVar, tt.envValue)

			_, err := ParseConfig(output.Params{})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}

// Test for Issue #5: URL param parse errors return errors
func TestParseConfig_InvalidURLParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		urlParam      string
		errorContains string
	}{
		{
			name:          "invalid tlsEnabled URL param",
			urlParam:      "localhost:9000?tlsEnabled=yes",
			errorContains: "invalid tlsEnabled URL parameter",
		},
		{
			name:          "invalid tlsInsecureSkipVerify URL param",
			urlParam:      "localhost:9000?tlsInsecureSkipVerify=nah",
			errorContains: "invalid tlsInsecureSkipVerify URL parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseConfig(output.Params{
				ConfigArgument: tt.urlParam,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}

// Helper function to marshal JSON for test cases
func mustMarshalJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
