package clickhouse

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"go.k6.io/k6/output"
)

// validIdentifierRegex matches valid ClickHouse identifiers
// Alphanumeric + underscore, 1-63 characters
var validIdentifierRegex = regexp.MustCompile(`^[a-zA-Z0-9_]{1,63}$`)

// isValidIdentifier validates ClickHouse identifier names
func isValidIdentifier(name string) bool {
	return validIdentifierRegex.MatchString(name)
}

// TLSConfig holds TLS/SSL configuration options
type TLSConfig struct {
	// Enabled controls whether TLS is enabled
	// Env: K6_CLICKHOUSE_TLS_ENABLED
	Enabled bool

	// InsecureSkipVerify disables certificate verification (INSECURE - use only for testing)
	// Env: K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY
	InsecureSkipVerify bool

	// CAFile is the path to a CA certificate file to append to the system pool
	// Env: K6_CLICKHOUSE_TLS_CA_FILE
	CAFile string

	// CertFile is the path to a client certificate file for mTLS
	// Env: K6_CLICKHOUSE_TLS_CERT_FILE
	CertFile string

	// KeyFile is the path to a client private key file for mTLS
	// Env: K6_CLICKHOUSE_TLS_KEY_FILE
	KeyFile string

	// ServerName is the server name for SNI (Server Name Indication)
	// Env: K6_CLICKHOUSE_TLS_SERVER_NAME
	ServerName string
}

// Config holds the ClickHouse output configuration
//
// Default values:
//   - Addr: "localhost:9000"
//   - User: "default"
//   - Password: "" (empty)
//   - Database: "k6"
//   - Table: "samples"
//   - PushInterval: 1s
//   - SchemaMode: "simple"
//   - SkipSchemaCreation: false
//   - RetryAttempts: 3
//   - RetryDelay: 100ms
//   - RetryMaxDelay: 5s
//   - BufferEnabled: true
//   - BufferMaxSamples: 10000
//   - BufferDropPolicy: "oldest"
//
// Configuration sources (in priority order):
//  1. Environment variables (K6_CLICKHOUSE_*)
//  2. URL parameters (e.g. --out clickhouse=...?param=value)
//  3. JSON config (in script options)
//  4. Default values
type Config struct {
	// Addr is the ClickHouse server address (host:port).
	// Env: K6_CLICKHOUSE_ADDR
	Addr string

	// User is the ClickHouse username.
	// Env: K6_CLICKHOUSE_USER
	User string

	// Password is the ClickHouse password.
	// Env: K6_CLICKHOUSE_PASSWORD
	Password string

	// Database is the database name to store metrics.
	// Env: K6_CLICKHOUSE_DB
	Database string

	// Table is the table name to store metrics.
	// Env: K6_CLICKHOUSE_TABLE
	Table string

	// PushInterval is how often to flush metrics to ClickHouse.
	// Env: K6_CLICKHOUSE_PUSH_INTERVAL (parsed as duration, e.g. "1s")
	PushInterval time.Duration

	// SchemaMode determines the table schema ("simple" or "compatible").
	// Env: K6_CLICKHOUSE_SCHEMA_MODE
	SchemaMode string

	// SkipSchemaCreation disables automatic database and table creation.
	// Env: K6_CLICKHOUSE_SKIP_SCHEMA_CREATION ("true" to skip)
	SkipSchemaCreation bool

	// TLS holds TLS/SSL configuration
	TLS TLSConfig

	// Retry settings for handling transient connection failures

	// RetryAttempts is the maximum number of retry attempts per flush operation.
	// Set to 0 for no retries (fail immediately). Default: 3
	// Env: K6_CLICKHOUSE_RETRY_ATTEMPTS
	RetryAttempts uint

	// RetryDelay is the initial delay between retry attempts.
	// Uses exponential backoff: delay * 2^attempt. Default: 100ms
	// Env: K6_CLICKHOUSE_RETRY_DELAY
	RetryDelay time.Duration

	// RetryMaxDelay is the maximum delay cap for exponential backoff. Default: 5s
	// Env: K6_CLICKHOUSE_RETRY_MAX_DELAY
	RetryMaxDelay time.Duration

	// Buffer settings for handling extended outages

	// BufferEnabled enables in-memory buffering of samples during connection failures.
	// When true, failed samples are queued and retried on next successful connection.
	// Default: true
	// Env: K6_CLICKHOUSE_BUFFER_ENABLED
	BufferEnabled bool

	// BufferMaxSamples is the maximum number of sample containers to buffer.
	// When exceeded, samples are dropped according to BufferDropPolicy.
	// Default: 10000
	// Env: K6_CLICKHOUSE_BUFFER_MAX_SAMPLES
	BufferMaxSamples int

	// BufferDropPolicy determines which samples to drop when buffer overflows.
	// Valid values: "oldest" (drop oldest, preserve recent) or "newest" (drop incoming).
	// Default: "oldest"
	// Env: K6_CLICKHOUSE_BUFFER_DROP_POLICY
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
	if _, err := GetSchema(c.SchemaMode); err != nil {
		return fmt.Errorf("invalid schemaMode: %s (available: %v)", c.SchemaMode, AvailableSchemas())
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
	if c.RetryDelay < 0 {
		return fmt.Errorf("retry delay must be non-negative, got %v", c.RetryDelay)
	}
	if c.RetryMaxDelay < 0 {
		return fmt.Errorf("retry max delay must be non-negative, got %v", c.RetryMaxDelay)
	}
	if c.RetryMaxDelay > 0 && c.RetryDelay > c.RetryMaxDelay {
		return fmt.Errorf("retry delay (%v) cannot exceed max delay (%v)", c.RetryDelay, c.RetryMaxDelay)
	}

	// Validate buffer configuration
	if c.BufferEnabled && c.BufferMaxSamples <= 0 {
		return fmt.Errorf("buffer max samples must be positive when buffering is enabled, got %d", c.BufferMaxSamples)
	}
	if c.BufferDropPolicy != "" && c.BufferDropPolicy != "oldest" && c.BufferDropPolicy != "newest" {
		return fmt.Errorf("invalid buffer drop policy: %s (valid: oldest, newest)", c.BufferDropPolicy)
	}

	return nil
}

// NewConfig returns a Config with default values
func NewConfig() Config {
	return Config{
		Addr:               "localhost:9000",
		User:               "default",
		Password:           "",
		Database:           "k6",
		Table:              "samples",
		PushInterval:       1 * time.Second,
		SchemaMode:         "simple",
		SkipSchemaCreation: false,
		TLS: TLSConfig{
			Enabled:            false,
			InsecureSkipVerify: false,
			CAFile:             "",
			CertFile:           "",
			KeyFile:            "",
			ServerName:         "",
		},
		// Retry defaults: 3 attempts with exponential backoff (100ms, 200ms, 400ms...)
		RetryAttempts: 3,
		RetryDelay:    100 * time.Millisecond,
		RetryMaxDelay: 5 * time.Second,
		// Buffer defaults: enabled with 10K sample capacity, drop oldest on overflow
		BufferEnabled:    true,
		BufferMaxSamples: 10000,
		BufferDropPolicy: "oldest",
	}
}

// ParseConfig parses the configuration from output.Params
//
//nolint:gocyclo // complexity is acceptable for parsing multiple config sources
func ParseConfig(params output.Params) (Config, error) {
	cfg := NewConfig()

	// Parse JSON config if provided
	if params.JSONConfig != nil {
		jsonConf := struct {
			Addr               string `json:"addr"`
			User               string `json:"user"`
			Password           string `json:"password"`
			Database           string `json:"database"`
			Table              string `json:"table"`
			PushInterval       string `json:"pushInterval"`
			SchemaMode         string `json:"schemaMode"`
			SkipSchemaCreation *bool  `json:"skipSchemaCreation"` // Pointer to distinguish unset from false
			TLS                *struct {
				Enabled            bool   `json:"enabled"`
				InsecureSkipVerify bool   `json:"insecureSkipVerify"`
				CAFile             string `json:"caFile"`
				CertFile           string `json:"certFile"`
				KeyFile            string `json:"keyFile"`
				ServerName         string `json:"serverName"`
			} `json:"tls"`
			// Retry configuration
			RetryAttempts *uint  `json:"retryAttempts"` // Pointer to distinguish unset from 0
			RetryDelay    string `json:"retryDelay"`
			RetryMaxDelay string `json:"retryMaxDelay"`
			// Buffer configuration
			BufferEnabled    *bool  `json:"bufferEnabled"`    // Pointer to distinguish unset from false
			BufferMaxSamples *int   `json:"bufferMaxSamples"` // Pointer to distinguish unset from 0
			BufferDropPolicy string `json:"bufferDropPolicy"`
		}{}

		if err := json.Unmarshal(params.JSONConfig, &jsonConf); err != nil {
			return cfg, fmt.Errorf("failed to parse json config: %w", err)
		}

		if jsonConf.Addr != "" {
			cfg.Addr = jsonConf.Addr
		}
		if jsonConf.User != "" {
			cfg.User = jsonConf.User
		}
		if jsonConf.Password != "" {
			cfg.Password = jsonConf.Password
		}
		if jsonConf.Database != "" {
			cfg.Database = jsonConf.Database
		}
		if jsonConf.Table != "" {
			cfg.Table = jsonConf.Table
		}
		if jsonConf.PushInterval != "" {
			d, err := time.ParseDuration(jsonConf.PushInterval)
			if err != nil {
				return cfg, fmt.Errorf("invalid pushInterval: %w", err)
			}
			cfg.PushInterval = d
		}
		if jsonConf.SchemaMode != "" {
			cfg.SchemaMode = jsonConf.SchemaMode
		}
		if jsonConf.SkipSchemaCreation != nil {
			cfg.SkipSchemaCreation = *jsonConf.SkipSchemaCreation
		}
		// Parse TLS config
		if jsonConf.TLS != nil {
			cfg.TLS.Enabled = jsonConf.TLS.Enabled
			cfg.TLS.InsecureSkipVerify = jsonConf.TLS.InsecureSkipVerify
			if jsonConf.TLS.CAFile != "" {
				cfg.TLS.CAFile = jsonConf.TLS.CAFile
			}
			if jsonConf.TLS.CertFile != "" {
				cfg.TLS.CertFile = jsonConf.TLS.CertFile
			}
			if jsonConf.TLS.KeyFile != "" {
				cfg.TLS.KeyFile = jsonConf.TLS.KeyFile
			}
			if jsonConf.TLS.ServerName != "" {
				cfg.TLS.ServerName = jsonConf.TLS.ServerName
			}
		}
		// Parse retry config
		if jsonConf.RetryAttempts != nil {
			cfg.RetryAttempts = *jsonConf.RetryAttempts
		}
		if jsonConf.RetryDelay != "" {
			d, err := time.ParseDuration(jsonConf.RetryDelay)
			if err != nil {
				return cfg, fmt.Errorf("invalid retryDelay: %w", err)
			}
			cfg.RetryDelay = d
		}
		if jsonConf.RetryMaxDelay != "" {
			d, err := time.ParseDuration(jsonConf.RetryMaxDelay)
			if err != nil {
				return cfg, fmt.Errorf("invalid retryMaxDelay: %w", err)
			}
			cfg.RetryMaxDelay = d
		}
		// Parse buffer config
		if jsonConf.BufferEnabled != nil {
			cfg.BufferEnabled = *jsonConf.BufferEnabled
		}
		if jsonConf.BufferMaxSamples != nil {
			cfg.BufferMaxSamples = *jsonConf.BufferMaxSamples
		}
		if jsonConf.BufferDropPolicy != "" {
			cfg.BufferDropPolicy = jsonConf.BufferDropPolicy
		}
	}

	// Parse URL config if provided (--out clickhouse=addr?database=foo)
	if params.ConfigArgument != "" {
		u, err := url.Parse(params.ConfigArgument)
		if err == nil {
			if u.Host != "" {
				cfg.Addr = u.Host
			} else if u.Path != "" {
				cfg.Addr = u.Path
			}

			q := u.Query()
			if user := q.Get("user"); user != "" {
				cfg.User = user
			}
			if password := q.Get("password"); password != "" {
				cfg.Password = password
			}
			if db := q.Get("database"); db != "" {
				cfg.Database = db
			}
			if table := q.Get("table"); table != "" {
				cfg.Table = table
			}
			if schemaMode := q.Get("schemaMode"); schemaMode != "" {
				cfg.SchemaMode = schemaMode
			}
			if skipSchema := q.Get("skipSchemaCreation"); skipSchema != "" {
				cfg.SkipSchemaCreation = skipSchema == "true"
			}

			// Parse TLS URL parameters
			if tlsEnabled := q.Get("tlsEnabled"); tlsEnabled != "" {
				enabled, err := strconv.ParseBool(tlsEnabled)
				if err != nil {
					return cfg, fmt.Errorf("invalid tlsEnabled URL parameter value %q: %w", tlsEnabled, err)
				}
				cfg.TLS.Enabled = enabled
			}
			if tlsInsecure := q.Get("tlsInsecureSkipVerify"); tlsInsecure != "" {
				insecure, err := strconv.ParseBool(tlsInsecure)
				if err != nil {
					return cfg, fmt.Errorf("invalid tlsInsecureSkipVerify URL parameter value %q: %w", tlsInsecure, err)
				}
				cfg.TLS.InsecureSkipVerify = insecure
			}
			if tlsCAFile := q.Get("tlsCAFile"); tlsCAFile != "" {
				cfg.TLS.CAFile = tlsCAFile
			}
			if tlsCertFile := q.Get("tlsCertFile"); tlsCertFile != "" {
				cfg.TLS.CertFile = tlsCertFile
			}
			if tlsKeyFile := q.Get("tlsKeyFile"); tlsKeyFile != "" {
				cfg.TLS.KeyFile = tlsKeyFile
			}
			if tlsServerName := q.Get("tlsServerName"); tlsServerName != "" {
				cfg.TLS.ServerName = tlsServerName
			}
		}
	}

	// Parse environment variables (highest priority)
	if addr := os.Getenv("K6_CLICKHOUSE_ADDR"); addr != "" {
		cfg.Addr = addr
	}
	if user := os.Getenv("K6_CLICKHOUSE_USER"); user != "" {
		cfg.User = user
	}
	if password := os.Getenv("K6_CLICKHOUSE_PASSWORD"); password != "" {
		cfg.Password = password
	}
	if db := os.Getenv("K6_CLICKHOUSE_DB"); db != "" {
		cfg.Database = db
	}
	if table := os.Getenv("K6_CLICKHOUSE_TABLE"); table != "" {
		cfg.Table = table
	}
	if schemaMode := os.Getenv("K6_CLICKHOUSE_SCHEMA_MODE"); schemaMode != "" {
		cfg.SchemaMode = schemaMode
	}
	if skipSchema := os.Getenv("K6_CLICKHOUSE_SKIP_SCHEMA_CREATION"); skipSchema != "" {
		cfg.SkipSchemaCreation = skipSchema == "true"
	}

	// Parse TLS environment variables
	if tlsEnabled := os.Getenv("K6_CLICKHOUSE_TLS_ENABLED"); tlsEnabled != "" {
		enabled, err := strconv.ParseBool(tlsEnabled)
		if err != nil {
			return cfg, fmt.Errorf("invalid K6_CLICKHOUSE_TLS_ENABLED value %q: %w", tlsEnabled, err)
		}
		cfg.TLS.Enabled = enabled
	}
	if tlsInsecure := os.Getenv("K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY"); tlsInsecure != "" {
		insecure, err := strconv.ParseBool(tlsInsecure)
		if err != nil {
			return cfg, fmt.Errorf("invalid K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY value %q: %w", tlsInsecure, err)
		}
		cfg.TLS.InsecureSkipVerify = insecure
	}
	if tlsCAFile := os.Getenv("K6_CLICKHOUSE_TLS_CA_FILE"); tlsCAFile != "" {
		cfg.TLS.CAFile = tlsCAFile
	}
	if tlsCertFile := os.Getenv("K6_CLICKHOUSE_TLS_CERT_FILE"); tlsCertFile != "" {
		cfg.TLS.CertFile = tlsCertFile
	}
	if tlsKeyFile := os.Getenv("K6_CLICKHOUSE_TLS_KEY_FILE"); tlsKeyFile != "" {
		cfg.TLS.KeyFile = tlsKeyFile
	}
	if tlsServerName := os.Getenv("K6_CLICKHOUSE_TLS_SERVER_NAME"); tlsServerName != "" {
		cfg.TLS.ServerName = tlsServerName
	}

	// Parse retry environment variables
	if retryAttempts := os.Getenv("K6_CLICKHOUSE_RETRY_ATTEMPTS"); retryAttempts != "" {
		v, err := strconv.ParseUint(retryAttempts, 10, 32)
		if err != nil {
			return cfg, fmt.Errorf("invalid K6_CLICKHOUSE_RETRY_ATTEMPTS value %q: %w", retryAttempts, err)
		}
		cfg.RetryAttempts = uint(v)
	}
	if retryDelay := os.Getenv("K6_CLICKHOUSE_RETRY_DELAY"); retryDelay != "" {
		d, err := time.ParseDuration(retryDelay)
		if err != nil {
			return cfg, fmt.Errorf("invalid K6_CLICKHOUSE_RETRY_DELAY value %q: %w", retryDelay, err)
		}
		cfg.RetryDelay = d
	}
	if retryMaxDelay := os.Getenv("K6_CLICKHOUSE_RETRY_MAX_DELAY"); retryMaxDelay != "" {
		d, err := time.ParseDuration(retryMaxDelay)
		if err != nil {
			return cfg, fmt.Errorf("invalid K6_CLICKHOUSE_RETRY_MAX_DELAY value %q: %w", retryMaxDelay, err)
		}
		cfg.RetryMaxDelay = d
	}

	// Parse buffer environment variables
	if bufferEnabled := os.Getenv("K6_CLICKHOUSE_BUFFER_ENABLED"); bufferEnabled != "" {
		enabled, err := strconv.ParseBool(bufferEnabled)
		if err != nil {
			return cfg, fmt.Errorf("invalid K6_CLICKHOUSE_BUFFER_ENABLED value %q: %w", bufferEnabled, err)
		}
		cfg.BufferEnabled = enabled
	}
	if bufferMaxSamples := os.Getenv("K6_CLICKHOUSE_BUFFER_MAX_SAMPLES"); bufferMaxSamples != "" {
		v, err := strconv.Atoi(bufferMaxSamples)
		if err != nil {
			return cfg, fmt.Errorf("invalid K6_CLICKHOUSE_BUFFER_MAX_SAMPLES value %q: %w", bufferMaxSamples, err)
		}
		cfg.BufferMaxSamples = v
	}
	if bufferDropPolicy := os.Getenv("K6_CLICKHOUSE_BUFFER_DROP_POLICY"); bufferDropPolicy != "" {
		cfg.BufferDropPolicy = bufferDropPolicy
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
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
