package clickhouse

import (
	"fmt"
	"sort"
	"sync"
)

// schemaRegistry holds all registered schema implementations.
// Protected by mutex to allow registration during init().
var (
	schemaRegistry   = make(map[string]SchemaImplementation)
	schemaRegistryMu sync.RWMutex
)

// RegisterSchema registers a schema implementation by name.
// Call this in init() to register custom schemas.
//
// Example:
//
//	func init() {
//	    clickhouse.RegisterSchema(clickhouse.SchemaImplementation{
//	        Name:      "custom",
//	        Schema:    MyCustomSchema{},
//	        Converter: MyCustomConverter{},
//	    })
//	}
func RegisterSchema(impl SchemaImplementation) {
	schemaRegistryMu.Lock()
	defer schemaRegistryMu.Unlock()

	if impl.Name == "" {
		panic("schema implementation name cannot be empty")
	}
	if impl.Schema == nil {
		panic(fmt.Sprintf("schema implementation %q has nil Schema", impl.Name))
	}
	if impl.Converter == nil {
		panic(fmt.Sprintf("schema implementation %q has nil Converter", impl.Name))
	}

	schemaRegistry[impl.Name] = impl
}

// GetSchema returns a registered schema implementation by name.
// Returns an error if the schema is not found.
func GetSchema(name string) (SchemaImplementation, error) {
	schemaRegistryMu.RLock()
	defer schemaRegistryMu.RUnlock()

	if impl, ok := schemaRegistry[name]; ok {
		return impl, nil
	}
	return SchemaImplementation{}, fmt.Errorf("unknown schema: %q (available: %v)", name, availableSchemasLocked())
}

// AvailableSchemas returns all registered schema names in sorted order.
func AvailableSchemas() []string {
	schemaRegistryMu.RLock()
	defer schemaRegistryMu.RUnlock()

	return availableSchemasLocked()
}

// availableSchemasLocked returns schema names without acquiring lock (caller must hold lock)
func availableSchemasLocked() []string {
	names := make([]string, 0, len(schemaRegistry))
	for name := range schemaRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
