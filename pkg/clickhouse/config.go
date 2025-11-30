package clickhouse

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"go.k6.io/k6/output"
)

// Config holds the ClickHouse output configuration
type Config struct {
	Addr         string
	Database     string
	Table        string
	PushInterval time.Duration
}

// NewConfig returns a Config with default values
func NewConfig() Config {
	return Config{
		Addr:         "localhost:9000",
		Database:     "k6",
		Table:        "samples",
		PushInterval: 1 * time.Second,
	}
}

// ParseConfig parses the configuration from output.Params
func ParseConfig(params output.Params) (Config, error) {
	cfg := NewConfig()

	// Parse JSON config if provided
	if params.JSONConfig != nil {
		jsonConf := struct {
			Addr         string `json:"addr"`
			Database     string `json:"database"`
			Table        string `json:"table"`
			PushInterval string `json:"pushInterval"`
		}{}

		if err := json.Unmarshal(params.JSONConfig, &jsonConf); err != nil {
			return cfg, fmt.Errorf("failed to parse JSON config: %w", err)
		}

		if jsonConf.Addr != "" {
			cfg.Addr = jsonConf.Addr
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
			if db := q.Get("database"); db != "" {
				cfg.Database = db
			}
			if table := q.Get("table"); table != "" {
				cfg.Table = table
			}
		}
	}

	return cfg, nil
}
