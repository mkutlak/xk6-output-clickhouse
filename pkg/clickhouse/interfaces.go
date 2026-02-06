package clickhouse

import (
	"context"
	"database/sql"

	"go.k6.io/k6/metrics"
)

// SchemaCreator creates and manages ClickHouse table schemas.
// Implement this interface to define custom table structures for your use case.
type SchemaCreator interface {
	// CreateSchema creates the database and table in ClickHouse.
	// It should be idempotent (safe to call multiple times).
	CreateSchema(ctx context.Context, db *sql.DB, database, table string) error

	// InsertQuery returns the INSERT statement for this schema.
	// The query should use ? placeholders for values.
	InsertQuery(database, table string) string

	// ColumnCount returns the number of columns in the schema.
	// Used for pre-allocating row buffers.
	ColumnCount() int
}

// SampleConverter converts k6 metric samples to rows for ClickHouse insertion.
// Implement this interface to customize how k6 tags map to your schema columns.
type SampleConverter interface {
	// Convert transforms a k6 sample into a row ([]any) for insertion.
	// The returned slice must match the column order from InsertQuery.
	// Returns an error if conversion fails (e.g., type parsing errors).
	Convert(ctx context.Context, sample metrics.Sample) ([]any, error)

	// Release returns pooled resources (e.g., maps, slices) after insertion.
	// Called after batch Commit completes to enable memory reuse.
	Release(row []any)
}

// SchemaImplementation bundles a schema creator with its corresponding converter.
// This ensures the schema and conversion logic are always kept in sync.
type SchemaImplementation struct {
	// Name is the identifier used in schemaMode configuration (e.g., "simple", "compatible")
	Name string

	// Schema handles table creation and INSERT query generation
	Schema SchemaCreator

	// Converter handles k6 sample to row conversion
	Converter SampleConverter
}
