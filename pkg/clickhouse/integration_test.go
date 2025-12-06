package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clickhouseModule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

func TestIntegration_ClickHouse(t *testing.T) {
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
	defer func() {
		if err := clickhouseContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	// Get container endpoint
	endpoint, err := clickhouseContainer.PortEndpoint(ctx, "9000/tcp", "")
	require.NoError(t, err)

	t.Logf("ClickHouse running at %s", endpoint)

	// Config for the output
	dbName := "k6"
	tableName := "samples"

	// We need to create the database first because the output expects it (unless using default)
	// The container starts with 'default' database. The output tries to create schema in the configured DB.
	// Let's connect and create the 'k6' database.

	// Connect to ClickHouse to prepare DB
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{endpoint},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "password",
		},
	})
	require.NoError(t, err)

	err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	// Create the Output
	params := output.Params{
		JSONConfig: mustMarshalJSON(map[string]interface{}{
			"addr":         endpoint,
			"user":         "default",
			"password":     "password",
			"database":     dbName,
			"table":        tableName,
			"pushInterval": "100ms", // Fast flush for test
			"schemaMode":   "simple",
		}),
	}

	out, err := New(params)
	require.NoError(t, err)

	// Start the output
	err = out.Start()
	require.NoError(t, err)
	defer func() { require.NoError(t, out.Stop()) }()

	// Create a sample
	registry := metrics.NewRegistry()
	metric := registry.MustNewMetric("test_metric", metrics.Trend)

	now := time.Now()
	sample := metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: metric,
			Tags:   registry.RootTagSet().WithTagsFromMap(map[string]string{"tag1": "value1"}),
		},
		Time:  now,
		Value: 123.45,
	}

	// Add sample
	out.AddMetricSamples([]metrics.SampleContainer{
		testSampleContainer{
			samples: []metrics.Sample{sample},
		},
	})

	// Wait for flush (pushInterval is 100ms)
	time.Sleep(1 * time.Second)

	// Verify data in ClickHouse
	verifyDB, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://default:password@%s/%s", endpoint, dbName))
	require.NoError(t, err)
	defer func() { require.NoError(t, verifyDB.Close()) }()

	var count int
	err = verifyDB.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Should have 1 row")

	var metricName string
	var metricValue float64
	var tags map[string]string

	// In simple schema: timestamp, metric, value, tags
	err = verifyDB.QueryRowContext(ctx, fmt.Sprintf("SELECT metric, value, tags FROM %s", tableName)).Scan(&metricName, &metricValue, &tags)
	require.NoError(t, err)

	assert.Equal(t, "test_metric", metricName)
	assert.Equal(t, 123.45, metricValue)
	assert.Equal(t, "value1", tags["tag1"])
}

type testSampleContainer struct {
	samples []metrics.Sample
}

func (c testSampleContainer) GetSamples() []metrics.Sample {
	return c.samples
}
