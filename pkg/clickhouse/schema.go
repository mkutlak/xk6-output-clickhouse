package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
)

const (
	// TimestampPrecision is the precision for DateTime64 (3 = milliseconds)
	TimestampPrecision = 3
)

func createSchema(db *sql.DB, database, table string) error {
	ctx := context.Background()

	// Defense-in-depth: Validate identifiers before using them
	// Even though validation happens in config.go, schema.go should independently validate
	if !isValidIdentifier(database) {
		return fmt.Errorf("invalid database name: %s (must be alphanumeric + underscore, max 63 chars)", database)
	}
	if !isValidIdentifier(table) {
		return fmt.Errorf("invalid table name: %s (must be alphanumeric + underscore, max 63 chars)", table)
	}

	// Create database with properly escaped identifier
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", escapeIdentifier(database)))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create table with properly escaped identifiers
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
