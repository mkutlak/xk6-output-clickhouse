package clickhouse

import (
	"fmt"

	"go.k6.io/k6/v2/metrics"
)

func init() {
	RegisterSchema("simple", simpleSchema{})
}

// simpleSchema is the default schema. It stores all tags in a single
// Map(String, String) column, so any tag set fits without schema changes.
type simpleSchema struct{}

func (simpleSchema) CreateTable(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			timestamp DateTime64(3),
			metric LowCardinality(String),
			value Float64,
			tags Map(String, String)
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMMDD(timestamp)
		ORDER BY (metric, timestamp)
	`, table)
}

func (simpleSchema) InsertQuery(table string) string {
	return fmt.Sprintf("INSERT INTO %s (timestamp, metric, value, tags) VALUES (?, ?, ?, ?)", table)
}

func (simpleSchema) Row(s metrics.Sample) ([]any, error) {
	return []any{s.Time, s.Metric.Name, s.Value, tagMap(s)}, nil
}

// tagMap returns a fresh map of the sample's tags that the caller may modify.
func tagMap(s metrics.Sample) map[string]string {
	if s.Tags == nil {
		return map[string]string{}
	}
	return s.Tags.Map()
}
