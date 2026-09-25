package clickhouse

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"go.k6.io/k6/v2/metrics"
)

// Schema defines a table layout and how k6 samples map onto it.
type Schema interface {
	// CreateTable returns a CREATE TABLE IF NOT EXISTS statement for table,
	// which is passed already quoted as `database`.`table`.
	CreateTable(table string) string
	// InsertQuery returns an INSERT statement for table with one ? per column.
	InsertQuery(table string) string
	// Row converts a sample into column values, in InsertQuery column order.
	Row(sample metrics.Sample) ([]any, error)
}

// schemaRegistry holds all registered schemas, keyed by schemaMode name.
var (
	schemaRegistry   = make(map[string]Schema)
	schemaRegistryMu sync.RWMutex
)

// RegisterSchema makes a schema selectable through the schemaMode option.
// Call it from init(); registering an existing name replaces that schema.
//
// Example:
//
//	func init() {
//	    clickhouse.RegisterSchema("custom", MySchema{})
//	}
func RegisterSchema(name string, s Schema) {
	if name == "" {
		panic("schema name cannot be empty")
	}
	if s == nil {
		panic(fmt.Sprintf("schema %q is nil", name))
	}

	schemaRegistryMu.Lock()
	defer schemaRegistryMu.Unlock()
	schemaRegistry[name] = s
}

// getSchema returns the schema registered under name.
func getSchema(name string) (Schema, error) {
	schemaRegistryMu.RLock()
	s, ok := schemaRegistry[name]
	schemaRegistryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown schemaMode %q (available: %s)", name, strings.Join(availableSchemas(), ", "))
	}
	return s, nil
}

// availableSchemas returns all registered schema names in sorted order.
func availableSchemas() []string {
	schemaRegistryMu.RLock()
	defer schemaRegistryMu.RUnlock()

	return slices.Sorted(maps.Keys(schemaRegistry))
}
