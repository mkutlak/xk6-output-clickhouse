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
			SkipSchemaCreation bool   `json:"skipSchemaCreation"`
			TLS                *struct {
				Enabled            bool   `json:"enabled"`
				InsecureSkipVerify bool   `json:"insecureSkipVerify"`
				CAFile             string `json:"caFile"`
				CertFile           string `json:"certFile"`
				KeyFile            string `json:"keyFile"`
				ServerName         string `json:"serverName"`
			} `json:"tls"`
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
		if jsonConf.SkipSchemaCreation {
			cfg.SkipSchemaCreation = jsonConf.SkipSchemaCreation
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
			if skipSchema := q.Get("skipSchemaCreation"); skipSchema == "true" {
				cfg.SkipSchemaCreation = true
			}

			// Parse TLS URL parameters
			if tlsEnabled := q.Get("tlsEnabled"); tlsEnabled != "" {
				if enabled, err := strconv.ParseBool(tlsEnabled); err == nil {
					cfg.TLS.Enabled = enabled
				}
			}
			if tlsInsecure := q.Get("tlsInsecureSkipVerify"); tlsInsecure != "" {
				if insecure, err := strconv.ParseBool(tlsInsecure); err == nil {
					cfg.TLS.InsecureSkipVerify = insecure
				}
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
	if skipSchema := os.Getenv("K6_CLICKHOUSE_SKIP_SCHEMA_CREATION"); skipSchema == "true" {
		cfg.SkipSchemaCreation = true
	}

	// Parse TLS environment variables
	if tlsEnabled := os.Getenv("K6_CLICKHOUSE_TLS_ENABLED"); tlsEnabled != "" {
		if enabled, err := strconv.ParseBool(tlsEnabled); err == nil {
			cfg.TLS.Enabled = enabled
		}
	}
	if tlsInsecure := os.Getenv("K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY"); tlsInsecure != "" {
		if insecure, err := strconv.ParseBool(tlsInsecure); err == nil {
			cfg.TLS.InsecureSkipVerify = insecure
		}
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
