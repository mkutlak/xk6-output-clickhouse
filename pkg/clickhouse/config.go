package clickhouse

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.k6.io/k6/v2/output"
)

// validIdentifierRegex matches valid ClickHouse identifiers
// Alphanumeric + underscore, 1-63 characters
var validIdentifierRegex = regexp.MustCompile(`^[a-zA-Z0-9_]{1,63}$`)

// isValidIdentifier validates ClickHouse identifier names
func isValidIdentifier(name string) bool {
	return validIdentifierRegex.MatchString(name)
}

// maxRetryAttempts caps Config.RetryAttempts. A sane upper bound prevents two
// footguns: a typo'd huge value stalling every flush (and hanging Stop()), and
// an integer overflow where flush() passes retry.Attempts(RetryAttempts+1) —
// MaxUint+1 wraps to 0, which retry-go interprets as INFINITE retry. See Validate().
const maxRetryAttempts = 100

// Valid values for Config.BufferDropPolicy.
const (
	dropOldest = "oldest"
	dropNewest = "newest"
)

// TLSConfig holds TLS/SSL configuration options
type TLSConfig struct {
	// Enabled controls whether TLS is enabled
	Enabled bool

	// InsecureSkipVerify disables certificate verification (INSECURE - use only for testing)
	InsecureSkipVerify bool

	// CAFile is the path to a CA certificate file to append to the system pool
	CAFile string

	// CertFile is the path to a client certificate file for mTLS
	CertFile string

	// KeyFile is the path to a client private key file for mTLS
	KeyFile string

	// ServerName is the server name for SNI (Server Name Indication)
	ServerName string
}

// Config holds the ClickHouse output configuration. NewConfig returns the
// defaults; options lists every key and its environment variable.
//
// Configuration sources (in priority order):
//  1. Environment variables (K6_CLICKHOUSE_*)
//  2. URL parameters (e.g. --out xk6-clickhouse=...?param=value)
//  3. JSON config file (collectors.xk6-clickhouse, via --config)
//  4. Default values
type Config struct {
	// Addr is the ClickHouse server address (host:port).
	Addr string

	// User is the ClickHouse username.
	User string

	// Password is the ClickHouse password.
	Password string

	// Database is the database name to store metrics.
	Database string

	// Table is the table name to store metrics.
	Table string

	// PushInterval is how often to flush metrics to ClickHouse.
	PushInterval time.Duration

	// SchemaMode determines the table schema ("simple" or "compatible").
	SchemaMode string

	// SkipSchemaCreation disables automatic database and table creation.
	SkipSchemaCreation bool

	// TLS holds TLS/SSL configuration
	TLS TLSConfig

	// RetryAttempts is the maximum number of retries per flush; 0 fails immediately.
	RetryAttempts uint

	// RetryDelay is the initial delay between retries, doubled on each attempt.
	RetryDelay time.Duration

	// RetryMaxDelay caps the exponential backoff delay.
	RetryMaxDelay time.Duration

	// BufferEnabled keeps samples from failed flushes in memory and retries
	// them on the next flush.
	BufferEnabled bool

	// BufferMaxSamples is the maximum number of samples to buffer.
	BufferMaxSamples int

	// BufferDropPolicy picks which samples to drop when the buffer is full:
	// "oldest" (keep recent samples) or "newest" (drop incoming samples).
	BufferDropPolicy string
}

// validateFileReadable checks if a file exists and is readable
func validateFileReadable(path string) error {
	if path == "" {
		return fmt.Errorf("file path is empty")
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file does not exist: %s", path)
		}
		return fmt.Errorf("cannot access file %s: %w", path, err)
	}

	if info.IsDir() {
		return fmt.Errorf("path is a directory, not a file: %s", path)
	}

	// Try to open the file to verify readability
	file, err := os.Open(path) // #nosec G304 - path is validated by caller
	if err != nil {
		return fmt.Errorf("file is not readable: %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	return nil
}

// Validate checks the configuration for validity
//
//nolint:gocyclo // complexity is acceptable for validation with many fields
func (c Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("clickhouse address is required")
	}

	if c.User == "" {
		return fmt.Errorf("clickhouse user is required")
	}

	if c.Database == "" {
		return fmt.Errorf("clickhouse database name is required")
	}

	if !isValidIdentifier(c.Database) {
		return fmt.Errorf("invalid database name: %s (must be alphanumeric + underscore, max 63 chars)", c.Database)
	}

	if c.Table == "" {
		return fmt.Errorf("clickhouse table name is required")
	}

	if !isValidIdentifier(c.Table) {
		return fmt.Errorf("invalid table name: %s (must be alphanumeric + underscore, max 63 chars)", c.Table)
	}

	if c.PushInterval <= 0 {
		return fmt.Errorf("push interval must be positive, got %v", c.PushInterval)
	}

	// Validate schema mode against registered implementations
	if _, err := getSchema(c.SchemaMode); err != nil {
		return fmt.Errorf("invalid schemaMode: %s (available: %v)", c.SchemaMode, availableSchemas())
	}

	// Validate TLS configuration
	if c.TLS.Enabled {
		// Validate CA certificate file if specified
		if c.TLS.CAFile != "" {
			if err := validateFileReadable(c.TLS.CAFile); err != nil {
				return fmt.Errorf("TLS CA file validation failed: %w", err)
			}
		}

		// Validate client certificate and key files
		// Both must be specified together, or neither
		hasCert := c.TLS.CertFile != ""
		hasKey := c.TLS.KeyFile != ""

		if hasCert != hasKey {
			return fmt.Errorf("TLS client certificate and key must be specified together")
		}

		if hasCert {
			if err := validateFileReadable(c.TLS.CertFile); err != nil {
				return fmt.Errorf("TLS client certificate file validation failed: %w", err)
			}
		}

		if hasKey {
			if err := validateFileReadable(c.TLS.KeyFile); err != nil {
				return fmt.Errorf("TLS client key file validation failed: %w", err)
			}
		}
	}

	// Validate retry configuration
	if c.RetryAttempts > maxRetryAttempts {
		return fmt.Errorf("retry attempts must not exceed %d, got %d", maxRetryAttempts, c.RetryAttempts)
	}
	if c.RetryDelay < 0 {
		return fmt.Errorf("retry delay must be non-negative, got %v", c.RetryDelay)
	}
	if c.RetryMaxDelay < 0 {
		return fmt.Errorf("retry max delay must be non-negative, got %v", c.RetryMaxDelay)
	}
	// A zero max delay disables the exponential-backoff cap, letting per-retry
	// delays grow without bound. Require a positive cap whenever retries with a
	// non-zero base delay are enabled, so a misconfigured "0" can't stall flushes.
	if c.RetryAttempts > 0 && c.RetryDelay > 0 && c.RetryMaxDelay == 0 {
		return fmt.Errorf("retry max delay must be positive when retries are enabled (got 0); set retryMaxDelay to cap exponential backoff")
	}
	if c.RetryMaxDelay > 0 && c.RetryDelay > c.RetryMaxDelay {
		return fmt.Errorf("retry delay (%v) cannot exceed max delay (%v)", c.RetryDelay, c.RetryMaxDelay)
	}

	// Validate buffer configuration
	if c.BufferEnabled && c.BufferMaxSamples <= 0 {
		return fmt.Errorf("buffer max samples must be positive when buffering is enabled, got %d", c.BufferMaxSamples)
	}
	if c.BufferDropPolicy != "" && c.BufferDropPolicy != dropOldest && c.BufferDropPolicy != dropNewest {
		return fmt.Errorf("invalid buffer drop policy: %s (valid: %s, %s)", c.BufferDropPolicy, dropOldest, dropNewest)
	}

	return nil
}

// NewConfig returns a Config with default values
func NewConfig() Config {
	return Config{
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
}

// envPrefix is the prefix shared by every option's environment variable.
const envPrefix = "K6_CLICKHOUSE_"

// option is a config key and its environment variable.
type option struct{ key, env string }

// options lists every config key with its environment variable, in the
// order shown in error messages.
var options = []option{
	{"addr", "K6_CLICKHOUSE_ADDR"},
	{"user", "K6_CLICKHOUSE_USER"},
	{"password", "K6_CLICKHOUSE_PASSWORD"},
	{"database", "K6_CLICKHOUSE_DB"},
	{"table", "K6_CLICKHOUSE_TABLE"},
	{"pushInterval", "K6_CLICKHOUSE_PUSH_INTERVAL"},
	{"schemaMode", "K6_CLICKHOUSE_SCHEMA_MODE"},
	{"skipSchemaCreation", "K6_CLICKHOUSE_SKIP_SCHEMA_CREATION"},
	{"tlsEnabled", "K6_CLICKHOUSE_TLS_ENABLED"},
	{"tlsInsecureSkipVerify", "K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY"},
	{"tlsCAFile", "K6_CLICKHOUSE_TLS_CA_FILE"},
	{"tlsCertFile", "K6_CLICKHOUSE_TLS_CERT_FILE"},
	{"tlsKeyFile", "K6_CLICKHOUSE_TLS_KEY_FILE"},
	{"tlsServerName", "K6_CLICKHOUSE_TLS_SERVER_NAME"},
	{"retryAttempts", "K6_CLICKHOUSE_RETRY_ATTEMPTS"},
	{"retryDelay", "K6_CLICKHOUSE_RETRY_DELAY"},
	{"retryMaxDelay", "K6_CLICKHOUSE_RETRY_MAX_DELAY"},
	{"bufferEnabled", "K6_CLICKHOUSE_BUFFER_ENABLED"},
	{"bufferMaxSamples", "K6_CLICKHOUSE_BUFFER_MAX_SAMPLES"},
	{"bufferDropPolicy", "K6_CLICKHOUSE_BUFFER_DROP_POLICY"},
}

// jsonTLSKeys maps the keys of the JSON config's "tls" object to option keys.
var jsonTLSKeys = map[string]string{
	"enabled":            "tlsEnabled",
	"insecureSkipVerify": "tlsInsecureSkipVerify",
	"caFile":             "tlsCAFile",
	"certFile":           "tlsCertFile",
	"keyFile":            "tlsKeyFile",
	"serverName":         "tlsServerName",
}

// isOption reports whether key is listed in options.
func isOption(key string) bool {
	return slices.ContainsFunc(options, func(o option) bool { return o.key == key })
}

// unknownOptionError reports a key that is not listed in options.
func unknownOptionError(key string) error {
	keys := make([]string, len(options))
	for i, o := range options {
		keys[i] = o.key
	}
	return fmt.Errorf("unknown option %q (valid options: %s)", key, strings.Join(keys, ", "))
}

// set parses value and assigns it to the option key. An empty value leaves
// the option unchanged.
//
//nolint:gocyclo // one case per option
func (c *Config) set(key, value string) error {
	if !isOption(key) {
		return unknownOptionError(key)
	}
	if value == "" {
		return nil
	}

	var err error
	switch key {
	case "addr":
		c.Addr = value
	case "user":
		c.User = value
	case "password":
		c.Password = value
	case "database":
		c.Database = value
	case "table":
		c.Table = value
	case "pushInterval":
		c.PushInterval, err = time.ParseDuration(value)
	case "schemaMode":
		c.SchemaMode = value
	case "skipSchemaCreation":
		c.SkipSchemaCreation, err = strconv.ParseBool(value)
	case "tlsEnabled":
		c.TLS.Enabled, err = strconv.ParseBool(value)
	case "tlsInsecureSkipVerify":
		c.TLS.InsecureSkipVerify, err = strconv.ParseBool(value)
	case "tlsCAFile":
		c.TLS.CAFile = value
	case "tlsCertFile":
		c.TLS.CertFile = value
	case "tlsKeyFile":
		c.TLS.KeyFile = value
	case "tlsServerName":
		c.TLS.ServerName = value
	case "retryAttempts":
		var n uint64
		n, err = strconv.ParseUint(value, 10, 32)
		c.RetryAttempts = uint(n)
	case "retryDelay":
		c.RetryDelay, err = time.ParseDuration(value)
	case "retryMaxDelay":
		c.RetryMaxDelay, err = time.ParseDuration(value)
	case "bufferEnabled":
		c.BufferEnabled, err = strconv.ParseBool(value)
	case "bufferMaxSamples":
		c.BufferMaxSamples, err = strconv.Atoi(value)
	case "bufferDropPolicy":
		c.BufferDropPolicy = value
	default:
		return unknownOptionError(key)
	}
	if err != nil {
		return fmt.Errorf("invalid %s value %q: %w", key, value, err)
	}
	return nil
}

// ParseConfig builds a Config from defaults, the JSON config, the config
// argument and the environment, each overriding the previous, and validates it.
func ParseConfig(params output.Params) (Config, error) {
	cfg := NewConfig()

	if params.JSONConfig != nil {
		if err := cfg.applyJSON(params.JSONConfig); err != nil {
			return cfg, fmt.Errorf("json config: %w", err)
		}
	}

	if params.ConfigArgument != "" {
		if err := cfg.applyArgument(params.ConfigArgument); err != nil {
			return cfg, fmt.Errorf("invalid --out argument: %w", err)
		}
	}

	for _, o := range options {
		if err := cfg.set(o.key, params.Environment[o.env]); err != nil {
			return cfg, fmt.Errorf("%s: %w", o.env, err)
		}
	}

	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// applyJSON applies the collectors.xk6-clickhouse object. TLS options may be
// nested in a "tls" object.
func (c *Config) applyJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}

	if raw, ok := fields["tls"]; ok {
		delete(fields, "tls")
		var tlsFields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &tlsFields); err != nil {
			return fmt.Errorf("invalid tls value: %w", err)
		}
		for name, v := range tlsFields {
			key, ok := jsonTLSKeys[name]
			if !ok {
				return fmt.Errorf("unknown tls option %q (valid tls options: %s)",
					name, strings.Join(slices.Sorted(maps.Keys(jsonTLSKeys)), ", "))
			}
			if _, dup := fields[key]; dup {
				return fmt.Errorf("both tls.%s and %s are set", name, key)
			}
			fields[key] = v
		}
	}

	for _, key := range slices.Sorted(maps.Keys(fields)) {
		if !isOption(key) {
			return unknownOptionError(key)
		}
		value, err := jsonScalar(fields[key])
		if err != nil {
			return fmt.Errorf("invalid %s value: %w", key, err)
		}
		if err := c.set(key, value); err != nil {
			return err
		}
	}
	return nil
}

// jsonScalar returns the option text of a JSON value: a string's contents, a
// number's or boolean's literal, or "" (unset) for null.
func jsonScalar(raw json.RawMessage) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	switch v := v.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case json.Number:
		return v.String(), nil
	default:
		return "", errors.New("must be a string, number, boolean or null")
	}
}

// applyArgument applies the --out argument: a bare "host:port[?query]", or a
// URL with a scheme whose host and query are used.
func (c *Config) applyArgument(arg string) error {
	// A bare "host:port" is not a URL — url.Parse would misread the host as a
	// scheme. Only parse as a URL when a scheme ("://") is present.
	addr, rawQuery, _ := strings.Cut(arg, "?")
	if strings.Contains(arg, "://") {
		u, err := url.Parse(arg)
		if err != nil {
			return err
		}
		addr, rawQuery = u.Host, u.RawQuery
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return err
	}
	if err := c.set("addr", addr); err != nil {
		return err
	}
	for _, key := range slices.Sorted(maps.Keys(query)) {
		if err := c.set(key, query.Get(key)); err != nil {
			return err
		}
	}
	return nil
}

// unknownEnvVars returns the sorted K6_CLICKHOUSE_* variables in env that
// match no option, such as misspelled names.
func unknownEnvVars(env map[string]string) []string {
	var unknown []string
	for name := range env {
		if strings.HasPrefix(name, envPrefix) &&
			!slices.ContainsFunc(options, func(o option) bool { return o.env == name }) {
			unknown = append(unknown, name)
		}
	}
	slices.Sort(unknown)
	return unknown
}

// BuildTLSConfig builds a *tls.Config from the TLSConfig settings
// Returns nil, nil if TLS is not enabled (valid nil value, not an error)
func (tc TLSConfig) BuildTLSConfig() (*tls.Config, error) {
	if !tc.Enabled {
		return nil, nil //nolint:nilnil // nil TLS config is valid when TLS is disabled
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: tc.InsecureSkipVerify, //nolint:gosec // G402: User-configurable option for testing purposes
		ServerName:         tc.ServerName,
	}

	// Start with system CA pool
	var certPool *x509.CertPool
	var err error

	certPool, err = x509.SystemCertPool()
	if err != nil {
		// On some systems (like Windows), SystemCertPool might not be available
		// Fall back to an empty pool
		certPool = x509.NewCertPool()
	}

	// Append custom CA certificate if provided
	if tc.CAFile != "" {
		caCert, err := os.ReadFile(tc.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file %s: %w", tc.CAFile, err)
		}

		if ok := certPool.AppendCertsFromPEM(caCert); !ok {
			return nil, fmt.Errorf("failed to parse CA certificate from %s: no valid certificates found", tc.CAFile)
		}
	}

	tlsConfig.RootCAs = certPool

	// Load client certificate and key if provided
	if tc.CertFile != "" && tc.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tc.CertFile, tc.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}
