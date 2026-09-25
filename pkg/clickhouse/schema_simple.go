package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.k6.io/k6/v2/metrics"
)

// SimpleSchemaImpl is the default simple schema implementation.
// It stores all tags in a Map(String, String) column for maximum flexibility.
var SimpleSchemaImpl = SchemaImplementation{
	Name:      "simple",
	Schema:    SimpleSchema{},
	Converter: SimpleConverter{},
}

func init() {
	RegisterSchema(SimpleSchemaImpl)
}

// SimpleSchema implements SchemaCreator for the simple (default) schema.
//
// Schema structure:
//
//	CREATE TABLE {db}.{table} (
//	    timestamp DateTime64(3),
//	    metric LowCardinality(String),
//	    value Float64,
//	    tags Map(String, String)
//	) ENGINE = MergeTree()
//	PARTITION BY toYYYYMMDD(timestamp)
//	ORDER BY (metric, timestamp)
type SimpleSchema struct{}

// CreateSchema creates the database and table for the simple schema.
func (s SimpleSchema) CreateSchema(ctx context.Context, db *sql.DB, database, table string) error {
	// Defense-in-depth: Validate identifiers before using them
	if !isValidIdentifier(database) {
		return fmt.Errorf("invalid database name: %s (must be alphanumeric + underscore, max 63 chars)", database)
	}
	if !isValidIdentifier(table) {
		return fmt.Errorf("invalid table name: %s (must be alphanumeric + underscore, max 63 chars)", table)
	}

	// Create database
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", escapeIdentifier(database)))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create table
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			timestamp DateTime64(%d),
			metric LowCardinality(String),
			value Float64,
			tags Map(String, String)
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMMDD(timestamp)
		ORDER BY (metric, timestamp)
	`, escapeIdentifier(database), escapeIdentifier(table), TimestampPrecision)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// InsertQuery returns the INSERT statement for the simple schema.
func (s SimpleSchema) InsertQuery(database, table string) string {
	return fmt.Sprintf(
		"INSERT INTO %s.%s (timestamp, metric, value, tags) VALUES (?, ?, ?, ?)",
		escapeIdentifier(database), escapeIdentifier(table))
}

// simpleSample represents a sample for the simple schema.
type simpleSample struct {
	Timestamp time.Time
	Metric    string
	Value     float64
	Tags      map[string]string
}

// convertToSimple converts a k6 sample to the simple schema format.
func convertToSimple(sample metrics.Sample) simpleSample {
	tags := map[string]string{}
	if sample.Tags != nil {
		tags = sample.Tags.Map()
	}

	return simpleSample{
		Timestamp: sample.Time,
		Metric:    sample.Metric.Name,
		Value:     sample.Value,
		Tags:      tags,
	}
}

// SimpleConverter implements SampleConverter for the simple schema.
// All k6 tags are stored as-is in the tags Map column.
type SimpleConverter struct{}

// Convert transforms a k6 sample into a row for the simple schema.
func (c SimpleConverter) Convert(ctx context.Context, sample metrics.Sample) ([]any, error) {
	ss := convertToSimple(sample)

	return []any{ss.Timestamp, ss.Metric, ss.Value, ss.Tags}, nil
}
