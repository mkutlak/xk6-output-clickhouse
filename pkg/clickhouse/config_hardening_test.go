package clickhouse

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/output"
)

// TestParseConfig_SkipSchemaCreation_URLBoolParsing pins the strconv.ParseBool
// behavior for the skipSchemaCreation URL param, including the previously-broken
// truthy variants ("1", "TRUE", "t") and invalid values.
func TestParseConfig_SkipSchemaCreation_URLBoolParsing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		value     string
		want      bool
		wantError bool
	}{
		{"true", true, false},
		{"1", true, false},
		{"TRUE", true, false},
		{"t", true, false},
		{"false", false, false},
		{"0", false, false},
		{"maybe", false, true},
		{"yes", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			t.Parallel()

			cfg, err := ParseConfig(output.Params{
				ConfigArgument: "localhost:9000?skipSchemaCreation=" + tc.value,
			})
			if tc.wantError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "invalid skipSchemaCreation URL parameter value")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.SkipSchemaCreation)
		})
	}
}

// TestParseConfig_SkipSchemaCreation_EnvBoolParsing verifies the env var accepts
// truthy variants and rejects invalid values. Not parallel: t.Setenv mutates the
// process environment (and would panic under a parallel ancestor).
func TestParseConfig_SkipSchemaCreation_EnvBoolParsing(t *testing.T) {
	t.Run("env truthy variant", func(t *testing.T) {
		t.Setenv("K6_CLICKHOUSE_SKIP_SCHEMA_CREATION", "1")

		cfg, err := ParseConfig(output.Params{})
		require.NoError(t, err)
		assert.True(t, cfg.SkipSchemaCreation, "env value \"1\" should enable skipSchemaCreation")
	})

	t.Run("env invalid value errors", func(t *testing.T) {
		t.Setenv("K6_CLICKHOUSE_SKIP_SCHEMA_CREATION", "maybe")

		_, err := ParseConfig(output.Params{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid K6_CLICKHOUSE_SKIP_SCHEMA_CREATION")
	})
}

// TestConfig_Validate_RetryAttemptsBound guards against the overflow→infinite-retry
// footgun (MaxUint+1 wraps to 0 = infinite) and the large-value stall by capping
// RetryAttempts in Validate().
func TestConfig_Validate_RetryAttemptsBound(t *testing.T) {
	t.Parallel()

	t.Run("MaxUint is rejected (overflow guard)", func(t *testing.T) {
		t.Parallel()

		cfg := NewConfig()
		cfg.RetryAttempts = math.MaxUint

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "retry attempts must not exceed")
	})

	t.Run("just above cap is rejected", func(t *testing.T) {
		t.Parallel()

		cfg := NewConfig()
		cfg.RetryAttempts = maxRetryAttempts + 1

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "retry attempts must not exceed")
	})

	t.Run("at cap is allowed", func(t *testing.T) {
		t.Parallel()

		cfg := NewConfig()
		cfg.RetryAttempts = maxRetryAttempts

		assert.NoError(t, cfg.Validate())
	})
}

// TestConfig_Validate_RetryMaxDelayZero ensures a zero max delay (which disables
// the backoff cap) is rejected when retries with a non-zero base delay are enabled,
// but allowed when retries are disabled.
func TestConfig_Validate_RetryMaxDelayZero(t *testing.T) {
	t.Parallel()

	t.Run("zero max delay rejected when retries enabled", func(t *testing.T) {
		t.Parallel()

		cfg := NewConfig()
		cfg.RetryAttempts = 3
		cfg.RetryDelay = 100 * time.Millisecond
		cfg.RetryMaxDelay = 0

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "retry max delay must be positive when retries are enabled")
	})

	t.Run("zero max delay allowed when retries disabled", func(t *testing.T) {
		t.Parallel()

		cfg := NewConfig()
		cfg.RetryAttempts = 0
		cfg.RetryMaxDelay = 0

		assert.NoError(t, cfg.Validate())
	})
}

// TestParseConfig_EnvOverridesURL_CoreFields verifies the documented precedence
// (env > URL) for the core scalar fields, not just TLS/pushInterval.
func TestParseConfig_EnvOverridesURL_CoreFields(t *testing.T) {
	// NOT parallel: t.Setenv modifies process environment
	t.Setenv("K6_CLICKHOUSE_ADDR", "env-host:9000")
	t.Setenv("K6_CLICKHOUSE_USER", "env_user")
	t.Setenv("K6_CLICKHOUSE_DB", "env_db")
	t.Setenv("K6_CLICKHOUSE_TABLE", "env_table")

	cfg, err := ParseConfig(output.Params{
		ConfigArgument: "url-host:9000?user=url_user&database=url_db&table=url_table",
	})
	require.NoError(t, err)
	assert.Equal(t, "env-host:9000", cfg.Addr, "env addr should override URL addr")
	assert.Equal(t, "env_user", cfg.User, "env user should override URL user")
	assert.Equal(t, "env_db", cfg.Database, "env database should override URL database")
	assert.Equal(t, "env_table", cfg.Table, "env table should override URL table")
}

// TestParseConfig_CoreEnvVars verifies the happy path for the core K6_CLICKHOUSE_*
// scalar env vars (previously only PUSH_INTERVAL and error cases were covered).
func TestParseConfig_CoreEnvVars(t *testing.T) {
	// NOT parallel: t.Setenv modifies process environment
	t.Setenv("K6_CLICKHOUSE_ADDR", "ch.example.com:9000")
	t.Setenv("K6_CLICKHOUSE_USER", "k6user")
	t.Setenv("K6_CLICKHOUSE_DB", "metrics_db")
	t.Setenv("K6_CLICKHOUSE_TABLE", "metrics_tbl")
	t.Setenv("K6_CLICKHOUSE_SCHEMA_MODE", "compatible")

	cfg, err := ParseConfig(output.Params{})
	require.NoError(t, err)
	assert.Equal(t, "ch.example.com:9000", cfg.Addr)
	assert.Equal(t, "k6user", cfg.User)
	assert.Equal(t, "metrics_db", cfg.Database)
	assert.Equal(t, "metrics_tbl", cfg.Table)
	assert.Equal(t, "compatible", cfg.SchemaMode)
}

// TestConfig_Validate covers the non-TLS error branches of Validate() directly,
// constructing Configs from NewConfig() and mutating a single field to invalid.
func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mutate        func(c *Config)
		errorContains string
	}{
		{"empty addr", func(c *Config) { c.Addr = "" }, "address is required"},
		{"empty user", func(c *Config) { c.User = "" }, "user is required"},
		{"empty database", func(c *Config) { c.Database = "" }, "database name is required"},
		{"invalid database", func(c *Config) { c.Database = "bad-name!" }, "invalid database name"},
		{"empty table", func(c *Config) { c.Table = "" }, "table name is required"},
		{"invalid table", func(c *Config) { c.Table = "bad table" }, "invalid table name"},
		{"non-positive push interval", func(c *Config) { c.PushInterval = 0 }, "push interval must be positive"},
		{"invalid schema mode", func(c *Config) { c.SchemaMode = "nope" }, "invalid schemaMode"},
		{"negative retry delay", func(c *Config) { c.RetryDelay = -1 }, "retry delay must be non-negative"},
		{"retry delay exceeds max", func(c *Config) {
			c.RetryDelay = 10 * time.Second
			c.RetryMaxDelay = 5 * time.Second
		}, "cannot exceed max delay"},
		{"buffer max samples zero", func(c *Config) {
			c.BufferEnabled = true
			c.BufferMaxSamples = 0
		}, "buffer max samples must be positive"},
		{"invalid drop policy", func(c *Config) { c.BufferDropPolicy = "random" }, "invalid buffer drop policy"},
		{"retry attempts over cap", func(c *Config) { c.RetryAttempts = maxRetryAttempts + 1 }, "retry attempts must not exceed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := NewConfig()
			tt.mutate(&cfg)

			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}

	t.Run("default config is valid", func(t *testing.T) {
		t.Parallel()
		assert.NoError(t, NewConfig().Validate())
	})
}
