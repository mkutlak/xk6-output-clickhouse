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
			expectError: false,
		},
		{
			name: "json config overrides defaults",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
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
			expectError: false,
		},
		{
			name: "json config with partial overrides",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
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
			expectError: false,
		},
		{
			name: "json config with only table",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"table": "metrics_table",
				}),
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "metrics_table",
				PushInterval: 1 * time.Second,
			},
			expectError: false,
		},
		{
			name: "json config with only pushInterval",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"pushInterval": "10s",
				}),
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 10 * time.Second,
			},
			expectError: false,
		},
		{
			name: "json config with milliseconds pushInterval",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"pushInterval": "500ms",
				}),
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 500 * time.Millisecond,
			},
			expectError: false,
		},
		{
			name: "json config with minutes pushInterval",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"pushInterval": "2m",
				}),
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 2 * time.Minute,
			},
			expectError: false,
		},
		{
			name: "invalid json config",
			params: output.Params{
				JSONConfig: []byte(`{invalid json`),
			},
			expectError:   true,
			errorContains: "failed to parse JSON config",
		},
		{
			name: "invalid pushInterval format",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"pushInterval": "invalid",
				}),
			},
			expectError:   true,
			errorContains: "invalid pushInterval",
		},
		{
			name: "invalid pushInterval type",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"pushInterval": "not-a-duration",
				}),
			},
			expectError:   true,
			errorContains: "invalid pushInterval",
		},
		{
			name: "url config with address without scheme (treated as path)",
			params: output.Params{
				ConfigArgument: "clickhouse.example.com:9000",
			},
			expectedConfig: Config{
				Addr:         "localhost:9000", // Without scheme, url.Parse doesn't populate Host, defaults remain
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
			expectError: false,
		},
		{
			name: "url config with query parameters (no scheme)",
			params: output.Params{
				ConfigArgument: "clickhouse.example.com:9000?database=prod&table=metrics",
			},
			expectedConfig: Config{
				Addr:         "localhost:9000", // Without scheme, Host is empty, defaults remain for Addr
				Database:     "prod",          // But query params are parsed
				Table:        "metrics",
				PushInterval: 1 * time.Second,
			},
			expectError: false,
		},
		{
			name: "url config with only database query parameter",
			params: output.Params{
				ConfigArgument: "localhost:9000?database=test_db",
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "test_db",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
			expectError: false,
		},
		{
			name: "url config with only table query parameter",
			params: output.Params{
				ConfigArgument: "localhost:9000?table=test_table",
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "test_table",
				PushInterval: 1 * time.Second,
			},
			expectError: false,
		},
		{
			name: "url config with IP address (no scheme, treated as path)",
			params: output.Params{
				ConfigArgument: "192.168.1.100:9000",
			},
			expectedConfig: Config{
				Addr:         "localhost:9000", // Without scheme, defaults remain
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
			expectError: false,
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
			expectError: false,
		},
		{
			name: "json and url config - url query params override json",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"addr":         "json-host:9000",
					"database":     "json_db",
					"table":        "json_table",
					"pushInterval": "5s",
				}),
				ConfigArgument: "url-host:9000?database=url_db&table=url_table",
			},
			expectedConfig: Config{
				Addr:         "json-host:9000", // URL without scheme doesn't override addr
				Database:     "url_db",         // But query params do override
				Table:        "url_table",
				PushInterval: 5 * time.Second,
			},
			expectError: false,
		},
		{
			name: "url config with empty query parameters",
			params: output.Params{
				ConfigArgument: "localhost:9000?database=&table=",
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
			expectError: false,
		},
		{
			name: "json config with empty strings",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
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
			expectError: false,
		},
		{
			name: "url config with special characters in query",
			params: output.Params{
				ConfigArgument: "localhost:9000?database=k6-test&table=samples_2024",
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6-test",
				Table:        "samples_2024",
				PushInterval: 1 * time.Second,
			},
			expectError: false,
		},
		{
			name: "json config with zero pushInterval",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"pushInterval": "0s",
				}),
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 0,
			},
			expectError: false,
		},
		{
			name: "json config with negative pushInterval",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"pushInterval": "-5s",
				}),
			},
			expectedConfig: Config{
				Addr:         "localhost:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: -5 * time.Second,
			},
			expectError: false,
		},
		{
			name: "url config with IPv6 address (no scheme)",
			params: output.Params{
				ConfigArgument: "[::1]:9000",
			},
			expectedConfig: Config{
				Addr:         "localhost:9000", // Without scheme, defaults remain
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
			expectError: false,
		},
		{
			name: "json config with all fields set",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]interface{}{
					"addr":         "production-clickhouse:9000",
					"database":     "production",
					"table":        "performance_metrics",
					"pushInterval": "30s",
				}),
			},
			expectedConfig: Config{
				Addr:         "production-clickhouse:9000",
				Database:     "production",
				Table:        "performance_metrics",
				PushInterval: 30 * time.Second,
			},
			expectError: false,
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
			JSONConfig: mustMarshalJSON(map[string]interface{}{
				"addr": "json-configured:9000",
			}),
			ConfigArgument: "://invalid-url",
		}

		cfg, err := ParseConfig(params)
		require.NoError(t, err)
		assert.Equal(t, "json-configured:9000", cfg.Addr)
	})

	t.Run("url with fragment", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			ConfigArgument: "localhost:9000?database=test#fragment",
		}

		cfg, err := ParseConfig(params)
		require.NoError(t, err)
		assert.Equal(t, "localhost:9000", cfg.Addr)
		assert.Equal(t, "test", cfg.Database)
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

// Helper function to marshal JSON for test cases
func mustMarshalJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
