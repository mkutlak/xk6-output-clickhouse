package clickhouse

import (
	"context"
	"database/sql"
	"fmt"

	"go.k6.io/k6/metrics"
)

// CompatibleSchemaImpl is the legacy compatible schema implementation.
// It extracts known k6 tags into dedicated typed columns for query performance.
//
// This serves as an example of a custom schema implementation. Fork this file
// to create your own schema with the columns you need.
var CompatibleSchemaImpl = SchemaImplementation{
	Name:      "compatible",
	Schema:    CompatibleSchema{},
	Converter: CompatibleConverter{},
}

func init() {
	RegisterSchema(CompatibleSchemaImpl)
}

// CompatibleSchema implements SchemaCreator for the legacy compatible schema.
//
// Schema structure:
//
//	CREATE TABLE {db}.{table} (
//	    timestamp         DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
//	    metric            LowCardinality(String),
//	    metric_type       Enum8('counter'=1, 'gauge'=2, 'rate'=3, 'trend'=4),
//	    value             Float64 CODEC(Gorilla, ZSTD(1)),
//	    testid            LowCardinality(String) DEFAULT '',
//	    release           LowCardinality(String) DEFAULT '',
//	    scenario          LowCardinality(String) DEFAULT '',
//	    build_id          UInt32 DEFAULT 0 CODEC(Delta, ZSTD(1)),
//	    version           LowCardinality(String) DEFAULT '',
//	    branch            LowCardinality(String) DEFAULT 'master',
//	    name              String DEFAULT '' CODEC(ZSTD(1)),
//	    method            LowCardinality(String) DEFAULT '',
//	    status            UInt16 DEFAULT 0,
//	    expected_response Bool DEFAULT true,
//	    error_code        LowCardinality(String) DEFAULT '',
//	    rating            LowCardinality(String) DEFAULT '',
//	    resource_type     LowCardinality(String) DEFAULT '',
//	    ui_feature        LowCardinality(String) DEFAULT '',
//	    check_name        String DEFAULT '' CODEC(ZSTD(1)),
//	    group_name        LowCardinality(String) DEFAULT '',
//	    extra_tags        Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1))
//	) ENGINE = MergeTree()
//	PARTITION BY toYYYYMM(timestamp)
//	ORDER BY (metric, testid, release, timestamp)
//	TTL toDateTime(timestamp) + INTERVAL 365 DAY DELETE
//	SETTINGS index_granularity = 8192
type CompatibleSchema struct{}

// CreateSchema creates the database and table for the compatible schema.
func (s CompatibleSchema) CreateSchema(ctx context.Context, db *sql.DB, database, table string) error {
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

	// Create table with optimized schema
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			timestamp         DateTime64(%d, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
			metric            LowCardinality(String),
			metric_type       Enum8('counter'=1, 'gauge'=2, 'rate'=3, 'trend'=4),
			value             Float64 CODEC(Gorilla, ZSTD(1)),
			testid            LowCardinality(String) DEFAULT '',
			release           LowCardinality(String) DEFAULT '',
			scenario          LowCardinality(String) DEFAULT '',
			build_id          UInt32 DEFAULT 0 CODEC(Delta, ZSTD(1)),
			version           LowCardinality(String) DEFAULT '',
			branch            LowCardinality(String) DEFAULT 'master',
			name              String DEFAULT '' CODEC(ZSTD(1)),
			method            LowCardinality(String) DEFAULT '',
			status            UInt16 DEFAULT 0,
			expected_response Bool DEFAULT true,
			error_code        LowCardinality(String) DEFAULT '',
			rating            LowCardinality(String) DEFAULT '',
			resource_type     LowCardinality(String) DEFAULT '',
			ui_feature        LowCardinality(String) DEFAULT '',
			check_name        String DEFAULT '' CODEC(ZSTD(1)),
			group_name        LowCardinality(String) DEFAULT '',
			extra_tags        Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1))
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (metric, testid, release, timestamp)
		TTL toDateTime(timestamp) + INTERVAL 365 DAY DELETE
		SETTINGS index_granularity = 8192
	`, escapeIdentifier(database), escapeIdentifier(table), TimestampPrecision)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// InsertQuery returns the INSERT statement for the compatible schema.
func (s CompatibleSchema) InsertQuery(database, table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s.%s (
			timestamp, metric, metric_type, value,
			testid, release, scenario, build_id, version, branch,
			name, method, status, expected_response, error_code,
			rating, resource_type, ui_feature, check_name, group_name,
			extra_tags
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, escapeIdentifier(database), escapeIdentifier(table))
}

// ColumnCount returns the number of columns (21 for compatible schema).
func (s CompatibleSchema) ColumnCount() int {
	return 21
}

// CompatibleConverter implements SampleConverter for the compatible schema.
// It extracts known k6 tags into dedicated columns with type conversion.
type CompatibleConverter struct{}

// Convert transforms a k6 sample into a row for the compatible schema.
func (c CompatibleConverter) Convert(ctx context.Context, sample metrics.Sample) ([]any, error) {
	cs, err := ConvertToCompatible(ctx, sample)
	if err != nil {
		// Return tag map to pool even on error
		tagMapPool.Put(cs.ExtraTags)
		return nil, err
	}

	// Get row buffer from pool
	row := compatibleRowPool.Get().([]any)

	// Populate row buffer with sample data (order matches INSERT query)
	row[0] = cs.Timestamp
	row[1] = cs.Metric
	row[2] = cs.MetricType
	row[3] = cs.Value
	row[4] = cs.TestID
	row[5] = cs.Release
	row[6] = cs.Scenario
	row[7] = cs.BuildID
	row[8] = cs.Version
	row[9] = cs.Branch
	row[10] = cs.Name
	row[11] = cs.Method
	row[12] = cs.Status
	row[13] = cs.ExpectedResponse
	row[14] = cs.ErrorCode
	row[15] = cs.Rating
	row[16] = cs.ResourceType
	row[17] = cs.UIFeature
	row[18] = cs.CheckName
	row[19] = cs.GroupName
	row[20] = cs.ExtraTags

	return row, nil
}

// Release returns pooled resources after insertion.
func (c CompatibleConverter) Release(row []any) {
	// Return tag map to pool
	if len(row) > 20 {
		if tags, ok := row[20].(map[string]string); ok {
			tagMapPool.Put(tags)
		}
	}
	// Return row buffer to pool
	compatibleRowPool.Put(row) //nolint:staticcheck // SA6002: slice is reference type, safe to pass directly
}
