package clickhouse

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	clickhouseModule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// StartClickHouseContainer starts a ClickHouse container for testing
// Returns the endpoint address (host:port) and a cleanup function
func StartClickHouseContainer(t *testing.T) (endpoint string, cleanup func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start ClickHouse container
	clickhouseContainer, err := clickhouseModule.Run(ctx,
		"clickhouse/clickhouse-server:latest",
		clickhouseModule.WithUsername("default"),
		clickhouseModule.WithPassword("password"),
		clickhouseModule.WithDatabase("default"),
	)
	require.NoError(t, err)

	cleanup = func() {
		if err := clickhouseContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}

	// Get container endpoint
	endpoint, err = clickhouseContainer.PortEndpoint(ctx, "9000/tcp", "")
	if err != nil {
		cleanup()
		require.NoError(t, err)
	}

	t.Logf("ClickHouse running at %s", endpoint)
	return endpoint, cleanup
}

// CreateDatabase creates a database in the ClickHouse instance
func CreateDatabase(t *testing.T, endpoint, dbName string) {
	t.Helper()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{endpoint},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "password",
		},
	})
	require.NoError(t, err)

	err = conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

// GetTestConfig returns a valid config pointing to the given endpoint
func GetTestConfig(endpoint string) Config {
	return Config{
		Addr:         endpoint,
		Database:     "k6",
		Table:        "samples",
		User:         "default",
		Password:     "password",
		PushInterval: 100, // 100ms for fast tests
		SchemaMode:   "simple",
	}
}
