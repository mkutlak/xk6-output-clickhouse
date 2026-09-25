package clickhouse

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/output"
)

func TestNewConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

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
		expectedConfig config
		expectError    bool
		errorContains  string
	}{
		{
			name: "empty params returns defaults",
			params: output.Params{
				ConfigArgument: "",
				JSONConfig:     nil,
			},
			expectedConfig: config{
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
			expectedConfig: config{
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
			expectedConfig: config{
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
			errorContains: "json config: invalid character",
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
				ConfigArgument: "clickhouse://clickhouse.example.com:9000",
			},
			expectedConfig: config{
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
			expectedConfig: config{
				Addr:         "localhost:9000",
				Database:     "prod",
				Table:        "metrics",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "bare host:port argument sets addr",
			params: output.Params{
				ConfigArgument: "clickhouse-server:9000",
			},
			expectedConfig: config{
				Addr:         "clickhouse-server:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "bare ip:port argument sets addr",
			params: output.Params{
				ConfigArgument: "192.168.1.1:9000",
			},
			expectedConfig: config{
				Addr:         "192.168.1.1:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "bare ipv6:port argument sets addr",
			params: output.Params{
				ConfigArgument: "[::1]:9000",
			},
			expectedConfig: config{
				Addr:         "[::1]:9000",
				Database:     "k6",
				Table:        "samples",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "bare host:port argument with query params",
			params: output.Params{
				ConfigArgument: "db-host:9000?database=mydb&table=mytable",
			},
			expectedConfig: config{
				Addr:         "db-host:9000",
				Database:     "mydb",
				Table:        "mytable",
				PushInterval: 1 * time.Second,
			},
		},
		{
			name: "addr as query parameter",
			params: output.Params{
				ConfigArgument: "?addr=query-host:9000&database=prod",
			},
			expectedConfig: config{
				Addr:         "query-host:9000",
				Database:     "prod",
				Table:        "samples",
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
			expectedConfig: config{
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
			expectedConfig: config{
				Addr:         "url-host:9000",
				Database:     "url_db",
				Table:        "url_table",
				PushInterval: 5 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := parseConfig(tt.params)

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

	t.Run("malformed url config argument returns an error", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"addr": "json-configured:9000",
			}),
			ConfigArgument: "://invalid-url",
		}

		_, err := parseConfig(params)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid --out argument")
	})
}

func TestApplyArgument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arg  string

		wantErr        bool
		errContains    string
		errNotContains string
		wantAddr       string
		wantUser       string
		wantPassword   string
		wantDatabase   string
	}{
		{
			name:         "DSN with userinfo and path",
			arg:          "clickhouse://alice:s3cret@dbhost:9000/analytics",
			wantAddr:     "dbhost:9000",
			wantUser:     "alice",
			wantPassword: "s3cret",
			wantDatabase: "analytics",
		},
		{
			name:         "bare host:port/db",
			arg:          "dbhost:9000/analytics",
			wantAddr:     "dbhost:9000",
			wantUser:     "default",
			wantDatabase: "analytics",
		},
		{
			name:         "bare host:port still works",
			arg:          "dbhost:9000",
			wantAddr:     "dbhost:9000",
			wantUser:     "default",
			wantDatabase: "k6",
		},
		{
			name:         "query overrides path and userinfo",
			arg:          "clickhouse://alice:s3cret@dbhost:9000/analytics?user=bob&password=other&database=metrics",
			wantAddr:     "dbhost:9000",
			wantUser:     "bob",
			wantPassword: "other",
			wantDatabase: "metrics",
		},
		{
			name:         "percent-encoded password",
			arg:          "clickhouse://alice:p%40ss%23@dbhost:9000",
			wantAddr:     "dbhost:9000",
			wantUser:     "alice",
			wantPassword: "p@ss#",
			wantDatabase: "k6",
		},
		{
			name:        "fragment rejected",
			arg:         "clickhouse://dbhost:9000/analytics#frag",
			wantErr:     true,
			errContains: "%23",
		},
		{
			name:           "malformed path keeps password out of error",
			arg:            "clickhouse://alice:s3cretpw@dbhost:9000/%zz",
			wantErr:        true,
			errNotContains: "s3cretpw",
		},
		{
			name:           "invalid port keeps password out of error",
			arg:            "clickhouse://alice:s3cretpw@dbhost:abc",
			wantErr:        true,
			errNotContains: "s3cretpw",
		},
		{
			name:        "unsupported scheme",
			arg:         "http://dbhost:9000",
			wantErr:     true,
			errContains: `unsupported scheme "http"`,
		},
		{
			name:         "IPv6 host",
			arg:          "clickhouse://alice:s3cret@[::1]:9000/analytics",
			wantAddr:     "[::1]:9000",
			wantUser:     "alice",
			wantPassword: "s3cret",
			wantDatabase: "analytics",
		},
		{
			name:     "bare IPv6 host:port still works",
			arg:      "[::1]:9000",
			wantAddr: "[::1]:9000",
			wantUser: "default",
		},
		{
			name:        "multi-segment path rejected",
			arg:         "clickhouse://dbhost:9000/db/extra",
			wantErr:     true,
			errContains: "single database segment",
		},
		{
			name:         "'@' in a query value",
			arg:          "dbhost:9000?user=bob@corp",
			wantAddr:     "dbhost:9000",
			wantUser:     "bob@corp",
			wantDatabase: "k6",
		},
		{
			name:         "percent-encoded '/' and '?' in password",
			arg:          "clickhouse://u:p%2Fw%3Fx@host:9000",
			wantAddr:     "host:9000",
			wantUser:     "u",
			wantPassword: "p/w?x",
			wantDatabase: "k6",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultConfig()
			err := cfg.applyArgument(tt.arg)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				if tt.errNotContains != "" {
					assert.NotContains(t, err.Error(), tt.errNotContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantAddr, cfg.Addr)
			assert.Equal(t, tt.wantUser, cfg.User)
			assert.Equal(t, tt.wantPassword, cfg.Password)
			if tt.wantDatabase != "" {
				assert.Equal(t, tt.wantDatabase, cfg.Database)
			}
		})
	}
}

// TestParseConfig_MalformedDSNHidesPassword guards that an unencoded reserved
// character in the userinfo, which pushes the rest of the password into the
// host, path or a query key, never surfaces the password in an error.
func TestParseConfig_MalformedDSNHidesPassword(t *testing.T) {
	t.Parallel()

	for _, arg := range []string{
		"clickhouse://alice:s3cret/pw@dbhost:9000",
		"clickhouse://alice:123?topsecret@dbhost:9000",
		"clickhouse://alice:123/topsecret@dbhost:9000",
		"clickhouse://bob@alice:123/topsecret@dbhost:9000",
		"alice:123?topsecret@dbhost:9000",
	} {
		t.Run(arg, func(t *testing.T) {
			t.Parallel()

			_, err := parseConfig(output.Params{ConfigArgument: arg})
			require.ErrorIs(t, err, errMalformedDSN)
			assert.NotContains(t, err.Error(), "s3cret")
			assert.NotContains(t, err.Error(), "topsecret")
		})
	}
}

// TestApplyArgument_SchemeOnlyAtStart guards that a "://" later in the
// argument is not mistaken for a scheme.
func TestApplyArgument_SchemeOnlyAtStart(t *testing.T) {
	t.Parallel()

	for arg, want := range map[string]bool{
		"clickhouse://dbhost:9000":           true,
		"http://dbhost:9000":                 true,
		"dbhost:9000":                        false,
		"host:9000?tlsCAFile=file:///ca.pem": false,
		"alice:pa://ss@dbhost:9000":          false,
	} {
		assert.Equal(t, want, schemeRegex.MatchString(arg), arg)
	}

	cfg := defaultConfig()
	require.NoError(t, cfg.applyArgument("host:9000?tlsCAFile=file:///ca.pem"))
	assert.Equal(t, "host:9000", cfg.Addr)
	assert.Equal(t, "file:///ca.pem", cfg.TLS.CAFile)

	// Parsed as a bare host, the unencoded '/' in the password is reported
	// rather than a bogus "alice" scheme.
	cfg = defaultConfig()
	err := cfg.applyArgument("alice:pa://ss@dbhost:9000")
	require.ErrorIs(t, err, errMalformedDSN)
	assert.NotContains(t, err.Error(), "alice")
}

func TestConfig_Struct(t *testing.T) {
	t.Parallel()

	t.Run("config fields are settable", func(t *testing.T) {
		t.Parallel()

		cfg := config{
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

		var cfg config

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

		cfg, err := parseConfig(output.Params{
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
		cfg, err := parseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"skipSchemaCreation": false,
			}),
		})
		require.NoError(t, err)
		assert.False(t, cfg.SkipSchemaCreation, "skipSchemaCreation: false should be explicitly settable")
	})

	t.Run("skipSchemaCreation true explicitly set", func(t *testing.T) {
		t.Parallel()

		cfg, err := parseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"skipSchemaCreation": true,
			}),
		})
		require.NoError(t, err)
		assert.True(t, cfg.SkipSchemaCreation, "skipSchemaCreation: true should work")
	})

	t.Run("unset fields keep defaults", func(t *testing.T) {
		t.Parallel()

		cfg, err := parseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"addr": "custom:9000",
			}),
		})
		require.NoError(t, err)
		assert.Equal(t, uint(3), cfg.RetryAttempts, "unset retryAttempts should keep default")
		assert.Equal(t, 100000, cfg.BufferMaxSamples, "unset bufferMaxSamples should keep default")
		assert.False(t, cfg.SkipSchemaCreation, "unset skipSchemaCreation should keep default")
	})

	t.Run("bufferMaxSamples 0 is set but fails validation", func(t *testing.T) {
		t.Parallel()

		// bufferMaxSamples: 0 with bufferEnabled: true should fail validation
		_, err := parseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"bufferMaxSamples": 0,
			}),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "buffer max samples must be positive")
	})
}

// TestParseConfig_InvalidEnvVars verifies env parse errors name the variable.
func TestParseConfig_InvalidEnvVars(t *testing.T) {
	t.Parallel()

	tests := []struct{ envVar, envValue string }{
		{"K6_CLICKHOUSE_PUSH_INTERVAL", "not-a-duration"},
		{"K6_CLICKHOUSE_SKIP_SCHEMA_CREATION", "maybe"},
		{"K6_CLICKHOUSE_TLS_ENABLED", "yes"},
		{"K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY", "nope"},
		{"K6_CLICKHOUSE_RETRY_ATTEMPTS", "abc"},
		{"K6_CLICKHOUSE_RETRY_DELAY", "not-a-duration"},
		{"K6_CLICKHOUSE_RETRY_MAX_DELAY", "xyz"},
		{"K6_CLICKHOUSE_BUFFER_ENABLED", "maybe"},
		{"K6_CLICKHOUSE_BUFFER_MAX_SAMPLES", "lots"},
	}

	for _, tt := range tests {
		t.Run(tt.envVar, func(t *testing.T) {
			t.Parallel()

			_, err := parseConfig(output.Params{
				Environment: map[string]string{tt.envVar: tt.envValue},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.envVar+": invalid ")
			assert.Contains(t, err.Error(), `"`+tt.envValue+`"`)
		})
	}
}

// TestParseConfig_InvalidURLParams verifies URL param errors name the option.
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
			errorContains: `invalid --out argument: invalid tlsEnabled value "yes"`,
		},
		{
			name:          "invalid tlsInsecureSkipVerify URL param",
			urlParam:      "localhost:9000?tlsInsecureSkipVerify=nah",
			errorContains: "invalid --out argument: invalid tlsInsecureSkipVerify value",
		},
		{
			name:          "invalid pushInterval URL param",
			urlParam:      "localhost:9000?pushInterval=not-a-duration",
			errorContains: "invalid --out argument: invalid pushInterval value",
		},
		{
			name:          "invalid retryAttempts URL param",
			urlParam:      "localhost:9000?retryAttempts=abc",
			errorContains: "invalid --out argument: invalid retryAttempts value",
		},
		{
			name:          "invalid retryDelay URL param",
			urlParam:      "localhost:9000?retryDelay=not-a-duration",
			errorContains: "invalid --out argument: invalid retryDelay value",
		},
		{
			name:          "invalid retryMaxDelay URL param",
			urlParam:      "localhost:9000?retryMaxDelay=xyz",
			errorContains: "invalid --out argument: invalid retryMaxDelay value",
		},
		{
			name:          "invalid bufferEnabled URL param",
			urlParam:      "localhost:9000?bufferEnabled=maybe",
			errorContains: "invalid --out argument: invalid bufferEnabled value",
		},
		{
			name:          "invalid bufferMaxSamples URL param",
			urlParam:      "localhost:9000?bufferMaxSamples=lots",
			errorContains: "invalid --out argument: invalid bufferMaxSamples value",
		},
		{
			name:          "unknown URL param",
			urlParam:      "localhost:9000?databse=prod",
			errorContains: `invalid --out argument: unknown option "databse" (valid options: addr, user, password, database, table,`,
		},
		{
			name:          "URL param keys are case-sensitive",
			urlParam:      "localhost:9000?PushInterval=5s",
			errorContains: `invalid --out argument: unknown option "PushInterval"`,
		},
		{
			name:          "unknown URL param with empty value",
			urlParam:      "localhost:9000?tls=",
			errorContains: `invalid --out argument: unknown option "tls"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseConfig(output.Params{
				ConfigArgument: tt.urlParam,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}

func TestConfig_Set(t *testing.T) {
	t.Parallel()

	t.Run("every option has a setter", func(t *testing.T) {
		t.Parallel()

		for _, o := range options {
			cfg := defaultConfig()
			// "0" is a valid string, bool, integer and duration.
			assert.NoError(t, cfg.set(o.key, "0"), o.key)
		}
	})

	t.Run("empty value leaves the option unset", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		for _, o := range options {
			require.NoError(t, cfg.set(o.key, ""), o.key)
		}
		assert.Equal(t, defaultConfig(), cfg)
	})

	t.Run("unknown key lists valid options", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		err := cfg.set("databse", "k6")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `unknown option "databse" (valid options: addr, user, password, database, table, pushInterval,`)
		assert.Contains(t, err.Error(), ", bufferDropPolicy)")
	})
}

// TestParseConfig_EverySource sets every option from each source, including
// zero values that override non-zero defaults (retryAttempts, bufferEnabled).
func TestParseConfig_EverySource(t *testing.T) {
	t.Parallel()

	want := config{
		Addr:               "ch.example.com:9440",
		User:               "k6user",
		Password:           "secret",
		Database:           "metrics_db",
		Table:              "metrics_tbl",
		PushInterval:       5 * time.Second,
		SchemaMode:         "compatible",
		SkipSchemaCreation: true,
		TLS: tlsOptions{
			Enabled:            true,
			InsecureSkipVerify: true,
			CAFile:             testCACertFile,
			CertFile:           testClientCert,
			KeyFile:            testClientKey,
			ServerName:         "ch.example.com",
		},
		RetryAttempts:    0,
		RetryDelay:       200 * time.Millisecond,
		RetryMaxDelay:    10 * time.Second,
		BufferEnabled:    false,
		BufferMaxSamples: 5000,
		BufferDropPolicy: "newest",
	}

	query := url.Values{
		"addr":                  {"ch.example.com:9440"},
		"user":                  {"k6user"},
		"password":              {"secret"},
		"database":              {"metrics_db"},
		"table":                 {"metrics_tbl"},
		"pushInterval":          {"5s"},
		"schemaMode":            {"compatible"},
		"skipSchemaCreation":    {"true"},
		"tlsEnabled":            {"true"},
		"tlsInsecureSkipVerify": {"true"},
		"tlsCAFile":             {testCACertFile},
		"tlsCertFile":           {testClientCert},
		"tlsKeyFile":            {testClientKey},
		"tlsServerName":         {"ch.example.com"},
		"retryAttempts":         {"0"},
		"retryDelay":            {"200ms"},
		"retryMaxDelay":         {"10s"},
		"bufferEnabled":         {"false"},
		"bufferMaxSamples":      {"5000"},
		"bufferDropPolicy":      {"newest"},
	}

	sources := map[string]output.Params{
		"environment": {Environment: map[string]string{
			"K6_CLICKHOUSE_ADDR":                     "ch.example.com:9440",
			"K6_CLICKHOUSE_USER":                     "k6user",
			"K6_CLICKHOUSE_PASSWORD":                 "secret",
			"K6_CLICKHOUSE_DB":                       "metrics_db",
			"K6_CLICKHOUSE_TABLE":                    "metrics_tbl",
			"K6_CLICKHOUSE_PUSH_INTERVAL":            "5s",
			"K6_CLICKHOUSE_SCHEMA_MODE":              "compatible",
			"K6_CLICKHOUSE_SKIP_SCHEMA_CREATION":     "true",
			"K6_CLICKHOUSE_TLS_ENABLED":              "true",
			"K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY": "true",
			"K6_CLICKHOUSE_TLS_CA_FILE":              testCACertFile,
			"K6_CLICKHOUSE_TLS_CERT_FILE":            testClientCert,
			"K6_CLICKHOUSE_TLS_KEY_FILE":             testClientKey,
			"K6_CLICKHOUSE_TLS_SERVER_NAME":          "ch.example.com",
			"K6_CLICKHOUSE_RETRY_ATTEMPTS":           "0",
			"K6_CLICKHOUSE_RETRY_DELAY":              "200ms",
			"K6_CLICKHOUSE_RETRY_MAX_DELAY":          "10s",
			"K6_CLICKHOUSE_BUFFER_ENABLED":           "false",
			"K6_CLICKHOUSE_BUFFER_MAX_SAMPLES":       "5000",
			"K6_CLICKHOUSE_BUFFER_DROP_POLICY":       "newest",
		}},
		"url parameters": {ConfigArgument: "?" + query.Encode()},
		"json": {JSONConfig: mustMarshalJSON(map[string]any{
			"addr":               "ch.example.com:9440",
			"user":               "k6user",
			"password":           "secret",
			"database":           "metrics_db",
			"table":              "metrics_tbl",
			"pushInterval":       "5s",
			"schemaMode":         "compatible",
			"skipSchemaCreation": true,
			"tls": map[string]any{
				"enabled":            true,
				"insecureSkipVerify": true,
				"caFile":             testCACertFile,
				"certFile":           testClientCert,
				"keyFile":            testClientKey,
				"serverName":         "ch.example.com",
			},
			"retryAttempts":    0,
			"retryDelay":       "200ms",
			"retryMaxDelay":    "10s",
			"bufferEnabled":    false,
			"bufferMaxSamples": 5000,
			"bufferDropPolicy": "newest",
		})},
	}

	for name, params := range sources {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg, err := parseConfig(params)
			require.NoError(t, err)
			assert.Equal(t, want, cfg)
		})
	}
}

// TestParseConfig_Precedence verifies env > URL > JSON, and that an empty env
// value does not override.
func TestParseConfig_Precedence(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig(output.Params{
		JSONConfig: mustMarshalJSON(map[string]any{
			"user":     "json_user",
			"database": "json_db",
			"table":    "json_table",
		}),
		ConfigArgument: "?user=url_user&table=url_table",
		Environment: map[string]string{
			"K6_CLICKHOUSE_USER":  "env_user",
			"K6_CLICKHOUSE_TABLE": "",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "env_user", cfg.User)
	assert.Equal(t, "url_table", cfg.Table)
	assert.Equal(t, "json_db", cfg.Database)
}

func TestParseConfig_JSONValues(t *testing.T) {
	t.Parallel()

	t.Run("null leaves the option unset", func(t *testing.T) {
		t.Parallel()

		cfg, err := parseConfig(output.Params{
			JSONConfig: []byte(`{"database": null, "retryAttempts": null, "tls": null}`),
		})
		require.NoError(t, err)
		assert.Equal(t, defaultConfig(), cfg)
	})

	t.Run("strings are accepted for typed options", func(t *testing.T) {
		t.Parallel()

		cfg, err := parseConfig(output.Params{
			JSONConfig: []byte(`{"retryAttempts": "5", "bufferEnabled": "false", "tls": {"enabled": "true"}}`),
		})
		require.NoError(t, err)
		assert.Equal(t, uint(5), cfg.RetryAttempts)
		assert.False(t, cfg.BufferEnabled)
		assert.True(t, cfg.TLS.Enabled)
	})

	t.Run("keys match case-insensitively", func(t *testing.T) {
		t.Parallel()

		cfg, err := parseConfig(output.Params{
			JSONConfig: []byte(`{"PushInterval": "5s", "TLS": {"Enabled": true, "CAFile": "ca.pem"}, "TLSSERVERNAME": "ch"}`),
		})
		require.NoError(t, err)
		assert.Equal(t, 5*time.Second, cfg.PushInterval)
		assert.True(t, cfg.TLS.Enabled)
		assert.Equal(t, "ca.pem", cfg.TLS.CAFile)
		assert.Equal(t, "ch", cfg.TLS.ServerName)
	})

	errorCases := []struct{ name, json, errorContains string }{
		{"unknown key", `{"databse": "k6"}`, `json config: unknown option "databse" (valid options: addr, user,`},
		{"unknown tls key", `{"tls": {"ca": "ca.pem"}}`, `json config: unknown tls option "ca" (valid tls options: caFile, certFile, enabled, insecureSkipVerify, keyFile, serverName)`},
		{"tls not an object", `{"tls": true}`, "json config: invalid tls value"},
		{"tls key also set flat", `{"tlsEnabled": true, "tls": {"enabled": false}}`, "json config: both tls.enabled and tlsEnabled are set"},
		{"tls key also set flat, other case", `{"TLSEnabled": true, "tls": {"Enabled": false}}`, "json config: both tls.enabled and tlsEnabled are set"},
		{"key set twice in different case", `{"pushInterval": "1s", "PushInterval": "2s"}`, "json config: pushInterval is set more than once"},
		{"tls key set twice in different case", `{"tls": {"caFile": "a", "CAFile": "b"}}`, "json config: tls.caFile is set more than once"},
		{"tls set twice in different case", `{"tls": {}, "TLS": {}}`, "json config: tls is set more than once"},
		{"object value", `{"database": {"name": "k6"}}`, "json config: invalid database value: must be a string, number, boolean or null"},
		{"array value", `{"addr": ["a:9000", "b:9000"]}`, "json config: invalid addr value: must be a string, number, boolean or null"},
		{"fractional number", `{"retryAttempts": 1.5}`, `json config: invalid retryAttempts value "1.5"`},
		{"number for duration", `{"pushInterval": 5}`, `json config: invalid pushInterval value "5"`},
		{"not an object", `[1]`, "json config: json: cannot unmarshal array"},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseConfig(output.Params{JSONConfig: []byte(tt.json)})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}

func TestUnknownEnvVars(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"K6_CLICKHOUSE_DB":       "k6",
		"K6_CLICKHOUSE_TLS":      "true",
		"K6_CLICKHOUSE_DATABASE": "metrics",
		"K6_OUT":                 "xk6-clickhouse",
		"PATH":                   "/usr/bin",
	}
	assert.Equal(t, []string{"K6_CLICKHOUSE_DATABASE", "K6_CLICKHOUSE_TLS"}, unknownEnvVars(env))
	assert.Empty(t, unknownEnvVars(nil))

	// Unknown variables share the K6_ namespace, so parsing ignores them.
	cfg, err := parseConfig(output.Params{Environment: env})
	require.NoError(t, err)
	assert.Equal(t, "k6", cfg.Database)
}
