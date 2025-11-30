package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
)

func createSchema(db *sql.DB, database, table string) error {
	ctx := context.Background()

	// Create database
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create table
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			timestamp DateTime64(3),
			metric_name LowCardinality(String),
			metric_value Float64,
			tags Map(String, String)
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMMDD(timestamp)
		ORDER BY (metric_name, timestamp)
	`, database, table)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}
