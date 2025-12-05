package clickhouse

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
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

// Config holds the ClickHouse output configuration
type Config struct {
	Addr               string
	User               string
	Password           string
	Database           string
	Table              string
	PushInterval       time.Duration
	SchemaMode         string
	SkipSchemaCreation bool
}

// Validate checks the configuration for validity
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

	if c.SchemaMode != "simple" && c.SchemaMode != "compatible" {
		return fmt.Errorf("invalid schemaMode: %s (must be 'simple' or 'compatible')", c.SchemaMode)
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
	}
}

// ParseConfig parses the configuration from output.Params
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
		}{}

		if err := json.Unmarshal(params.JSONConfig, &jsonConf); err != nil {
			return cfg, fmt.Errorf("failed to parse JSON config: %w", err)
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

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}
