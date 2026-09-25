package clickhouse

import (
	"math"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/output"
)

func TestParseConfig_Defaults(t *testing.T) {
	t.Parallel()

	want := config{
		Addr:             "localhost:9000",
		User:             "default",
		Database:         "k6",
		Table:            "samples",
		PushInterval:     1 * time.Second,
		SchemaMode:       "simple",
		RetryAttempts:    3,
		RetryDelay:       100 * time.Millisecond,
		RetryMaxDelay:    5 * time.Second,
		BufferEnabled:    true,
		BufferMaxSamples: 100000,
		BufferDropPolicy: dropOldest,
	}
	assert.Equal(t, want, defaultConfig())

	t.Run("empty params returns defaults", func(t *testing.T) {
		t.Parallel()

		cfg, err := parseConfig(output.Params{})
		require.NoError(t, err)
		assert.Equal(t, want, cfg)
	})

	t.Run("partial JSON override keeps remaining defaults", func(t *testing.T) {
		t.Parallel()

		cfg, err := parseConfig(output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"addr":     "192.168.1.100:9000",
				"database": "custom_db",
			}),
		})
		require.NoError(t, err)
		assert.Equal(t, "192.168.1.100:9000", cfg.Addr)
		assert.Equal(t, "custom_db", cfg.Database)
		assert.Equal(t, "samples", cfg.Table)
		assert.Equal(t, 1*time.Second, cfg.PushInterval)
	})

	t.Run("JSON null leaves options at defaults", func(t *testing.T) {
		t.Parallel()

		cfg, err := parseConfig(output.Params{
			JSONConfig: []byte(`{"database": null, "retryAttempts": null, "tls": null}`),
		})
		require.NoError(t, err)
		assert.Equal(t, want, cfg)
	})
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
}

// TestParseConfig_EverySource verifies that every option can be set from
// every source (env, --out query, DSN parts, and JSON in its flat, nested-tls
// and mixed-case forms), including zero values that override non-zero
// defaults (retryAttempts, bufferEnabled).
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

	// dsnQuery carries every option except the four that a DSN sets via its
	// authority and path (addr, user, password, database).
	dsnQuery := url.Values{}
	for k, v := range query {
		dsnQuery[k] = v
	}
	delete(dsnQuery, "addr")
	delete(dsnQuery, "user")
	delete(dsnQuery, "password")
	delete(dsnQuery, "database")

	flatJSON := map[string]any{
		"addr":                  "ch.example.com:9440",
		"user":                  "k6user",
		"password":              "secret",
		"database":              "metrics_db",
		"table":                 "metrics_tbl",
		"pushInterval":          "5s",
		"schemaMode":            "compatible",
		"skipSchemaCreation":    true,
		"tlsEnabled":            true,
		"tlsInsecureSkipVerify": true,
		"tlsCAFile":             testCACertFile,
		"tlsCertFile":           testClientCert,
		"tlsKeyFile":            testClientKey,
		"tlsServerName":         "ch.example.com",
		"retryAttempts":         0,
		"retryDelay":            "200ms",
		"retryMaxDelay":         "10s",
		"bufferEnabled":         false,
		"bufferMaxSamples":      5000,
		"bufferDropPolicy":      "newest",
	}

	nestedJSON := map[string]any{
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
	}

	mixedCaseJSON := map[string]any{
		"ADDR":               "ch.example.com:9440",
		"User":               "k6user",
		"PassWord":           "secret",
		"DataBase":           "metrics_db",
		"TABLE":              "metrics_tbl",
		"pushinterval":       "5s",
		"SchemaMode":         "compatible",
		"SKIPSCHEMACREATION": true,
		"Tls": map[string]any{
			"ENABLED":            true,
			"InsecureSkipVerify": true,
			"cafile":             testCACertFile,
			"CertFile":           testClientCert,
			"KEYFILE":            testClientKey,
			"servername":         "ch.example.com",
		},
		"RetryAttempts":    0,
		"retrydelay":       "200ms",
		"RETRYMAXDELAY":    "10s",
		"BufferEnabled":    false,
		"bufferMaxSamples": 5000,
		"BUFFERDROPPOLICY": "newest",
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
		"--out query":     {ConfigArgument: "?" + query.Encode()},
		"dsn parts":       {ConfigArgument: "clickhouse://k6user:secret@ch.example.com:9440/metrics_db?" + dsnQuery.Encode()},
		"json flat":       {JSONConfig: mustMarshalJSON(flatJSON)},
		"json nested tls": {JSONConfig: mustMarshalJSON(nestedJSON)},
		"json mixed-case": {JSONConfig: mustMarshalJSON(mixedCaseJSON)},
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

// TestParseConfig_JSONStringScalars verifies that numbers and booleans may
// also be given as JSON strings.
func TestParseConfig_JSONStringScalars(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig(output.Params{
		JSONConfig: []byte(`{"retryAttempts": "5", "bufferEnabled": "false", "tls": {"enabled": "true"}}`),
	})
	require.NoError(t, err)
	assert.Equal(t, uint(5), cfg.RetryAttempts)
	assert.False(t, cfg.BufferEnabled)
	assert.True(t, cfg.TLS.Enabled)
}

// TestParseConfig_Precedence verifies env > --out > JSON > defaults,
// including TLS keys and zero values overriding truthy values set by a
// lower-priority source.
func TestParseConfig_Precedence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params output.Params
		check  func(t *testing.T, cfg config)
	}{
		{
			name: "env overrides url overrides json; empty env does not override",
			params: output.Params{
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
			},
			check: func(t *testing.T, cfg config) {
				t.Helper()
				assert.Equal(t, "env_user", cfg.User)
				assert.Equal(t, "url_table", cfg.Table)
				assert.Equal(t, "json_db", cfg.Database)
			},
		},
		{
			name: "env overrides url for addr and database",
			params: output.Params{
				ConfigArgument: "url-host:9000?database=url_db",
				Environment: map[string]string{
					"K6_CLICKHOUSE_ADDR": "env-host:9000",
					"K6_CLICKHOUSE_DB":   "env_db",
				},
			},
			check: func(t *testing.T, cfg config) {
				t.Helper()
				assert.Equal(t, "env-host:9000", cfg.Addr)
				assert.Equal(t, "env_db", cfg.Database)
			},
		},
		{
			name: "env overrides json for tls keys",
			params: output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{
					"tls": map[string]any{
						"enabled":    false,
						"serverName": "json.example.com",
					},
				}),
				Environment: map[string]string{
					"K6_CLICKHOUSE_TLS_ENABLED":     "true",
					"K6_CLICKHOUSE_TLS_SERVER_NAME": "env.example.com",
				},
			},
			check: func(t *testing.T, cfg config) {
				t.Helper()
				assert.True(t, cfg.TLS.Enabled)
				assert.Equal(t, "env.example.com", cfg.TLS.ServerName)
			},
		},
		{
			name: "env retryAttempts=0 overrides non-zero json",
			params: output.Params{
				JSONConfig:  mustMarshalJSON(map[string]any{"retryAttempts": 5}),
				Environment: map[string]string{"K6_CLICKHOUSE_RETRY_ATTEMPTS": "0"},
			},
			check: func(t *testing.T, cfg config) {
				t.Helper()
				assert.Equal(t, uint(0), cfg.RetryAttempts)
			},
		},
		{
			name: "url bufferEnabled=false overrides true json",
			params: output.Params{
				JSONConfig:     mustMarshalJSON(map[string]any{"bufferEnabled": true}),
				ConfigArgument: "?bufferEnabled=false",
			},
			check: func(t *testing.T, cfg config) {
				t.Helper()
				assert.False(t, cfg.BufferEnabled)
			},
		},
		{
			name: "env skipSchemaCreation=false overrides true json",
			params: output.Params{
				JSONConfig:  mustMarshalJSON(map[string]any{"skipSchemaCreation": true}),
				Environment: map[string]string{"K6_CLICKHOUSE_SKIP_SCHEMA_CREATION": "false"},
			},
			check: func(t *testing.T, cfg config) {
				t.Helper()
				assert.False(t, cfg.SkipSchemaCreation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := parseConfig(tt.params)
			require.NoError(t, err)
			tt.check(t, cfg)
		})
	}
}

// TestParseConfig_InvalidValues verifies that a bad value's error names the
// key (JSON, --out) or environment variable it came from, and that an
// unknown key lists the valid options.
func TestParseConfig_InvalidValues(t *testing.T) {
	t.Parallel()

	t.Run("environment", func(t *testing.T) {
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
	})

	t.Run("--out argument", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name          string
			urlParam      string
			errorContains []string
		}{
			{"invalid tlsEnabled", "localhost:9000?tlsEnabled=yes", []string{`invalid --out argument: invalid tlsEnabled value "yes"`}},
			{"invalid tlsInsecureSkipVerify", "localhost:9000?tlsInsecureSkipVerify=nah", []string{"invalid --out argument: invalid tlsInsecureSkipVerify value"}},
			{"invalid pushInterval", "localhost:9000?pushInterval=not-a-duration", []string{"invalid --out argument: invalid pushInterval value"}},
			{"invalid retryAttempts", "localhost:9000?retryAttempts=abc", []string{"invalid --out argument: invalid retryAttempts value"}},
			{"invalid retryDelay", "localhost:9000?retryDelay=not-a-duration", []string{"invalid --out argument: invalid retryDelay value"}},
			{"invalid retryMaxDelay", "localhost:9000?retryMaxDelay=xyz", []string{"invalid --out argument: invalid retryMaxDelay value"}},
			{"invalid bufferEnabled", "localhost:9000?bufferEnabled=maybe", []string{"invalid --out argument: invalid bufferEnabled value"}},
			{"invalid bufferMaxSamples", "localhost:9000?bufferMaxSamples=lots", []string{"invalid --out argument: invalid bufferMaxSamples value"}},
			{"invalid skipSchemaCreation", "localhost:9000?skipSchemaCreation=maybe", []string{"invalid --out argument: invalid skipSchemaCreation value"}},
			{"unknown param lists valid options", "localhost:9000?databse=prod", []string{
				`invalid --out argument: unknown option "databse" (valid options: addr, user, password, database, table,`,
				", bufferDropPolicy)",
			}},
			{"unknown param is case-sensitive", "localhost:9000?PushInterval=5s", []string{`invalid --out argument: unknown option "PushInterval"`}},
			{"unknown param with empty value", "localhost:9000?tls=", []string{`invalid --out argument: unknown option "tls"`}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				_, err := parseConfig(output.Params{ConfigArgument: tt.urlParam})
				require.Error(t, err)
				for _, want := range tt.errorContains {
					assert.Contains(t, err.Error(), want)
				}
			})
		}
	})

	t.Run("json", func(t *testing.T) {
		t.Parallel()

		t.Run("malformed syntax", func(t *testing.T) {
			t.Parallel()

			_, err := parseConfig(output.Params{JSONConfig: []byte(`{invalid json`)})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "json config: invalid character")
		})

		t.Run("a syntactically valid value failing validate() is wrapped by parseConfig", func(t *testing.T) {
			t.Parallel()

			_, err := parseConfig(output.Params{
				JSONConfig: mustMarshalJSON(map[string]any{"pushInterval": "0s"}),
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid configuration: push interval must be positive")
		})

		tests := []struct{ name, json, errorContains string }{
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
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				_, err := parseConfig(output.Params{JSONConfig: []byte(tt.json)})
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			})
		}
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
		{
			name:         "addr set via query parameter",
			arg:          "?addr=query-host:9000&database=prod",
			wantAddr:     "query-host:9000",
			wantUser:     "default",
			wantDatabase: "prod",
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

// TestParseConfig_MalformedOutArgument verifies that a --out argument which
// cannot be parsed as a URL at all (not just as a DSN) is still reported.
func TestParseConfig_MalformedOutArgument(t *testing.T) {
	t.Parallel()

	_, err := parseConfig(output.Params{
		JSONConfig:     mustMarshalJSON(map[string]any{"addr": "json-configured:9000"}),
		ConfigArgument: "://invalid-url",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid --out argument")
}

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mutate        func(c *config)
		errorContains string
	}{
		{"empty addr", func(c *config) { c.Addr = "" }, "address is required"},
		{"empty user", func(c *config) { c.User = "" }, "user is required"},
		{"empty database", func(c *config) { c.Database = "" }, "database name is required"},
		{"invalid database", func(c *config) { c.Database = "bad-name!" }, "invalid database name"},
		{"database with SQL injection", func(c *config) { c.Database = "k6'; DROP TABLE samples; --" }, "invalid database name"},
		{"empty table", func(c *config) { c.Table = "" }, "table name is required"},
		{"invalid table", func(c *config) { c.Table = "bad table" }, "invalid table name"},
		{"table with SQL injection", func(c *config) { c.Table = "samples'; DROP DATABASE k6; --" }, "invalid table name"},
		{"non-positive push interval", func(c *config) { c.PushInterval = 0 }, "push interval must be positive"},
		{"retry attempts just above cap", func(c *config) { c.RetryAttempts = maxRetryAttempts + 1 }, "retry attempts must not exceed"},
		{"retry attempts at math.MaxUint (overflow guard)", func(c *config) { c.RetryAttempts = math.MaxUint }, "retry attempts must not exceed"},
		{"negative retry delay", func(c *config) { c.RetryDelay = -1 }, "retry delay must be non-negative"},
		{"retry delay exceeds max delay", func(c *config) {
			c.RetryDelay = 10 * time.Second
			c.RetryMaxDelay = 5 * time.Second
		}, "cannot exceed max delay"},
		{"zero max delay when retries enabled", func(c *config) {
			c.RetryAttempts = 3
			c.RetryDelay = 100 * time.Millisecond
			c.RetryMaxDelay = 0
		}, "retry max delay must be positive when retries are enabled"},
		{"buffer max samples zero while buffering enabled", func(c *config) {
			c.BufferEnabled = true
			c.BufferMaxSamples = 0
		}, "buffer max samples must be positive"},
		{"invalid drop policy", func(c *config) { c.BufferDropPolicy = "random" }, "invalid buffer drop policy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultConfig()
			tt.mutate(&cfg)

			err := cfg.validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}

	valid := []struct {
		name   string
		mutate func(c *config)
	}{
		{"default config", func(*config) {}},
		{"retry attempts at cap", func(c *config) { c.RetryAttempts = maxRetryAttempts }},
		{"zero max delay when retries disabled", func(c *config) {
			c.RetryAttempts = 0
			c.RetryMaxDelay = 0
		}},
	}
	for _, tt := range valid {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultConfig()
			tt.mutate(&cfg)
			assert.NoError(t, cfg.validate())
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
