package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

func TestIntegration_ClickHouse(t *testing.T) {
	endpoint, cleanup := StartClickHouseContainer(t)
	defer cleanup()

	dbName := "k6"
	tableName := "samples"

	CreateDatabase(t, endpoint, dbName)

	params := output.Params{
		JSONConfig: mustMarshalJSON(map[string]interface{}{
			"addr":         endpoint,
			"user":         testUsername,
			"password":     testPassword,
			"database":     dbName,
			"table":        tableName,
			"pushInterval": "100ms",
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

	out.AddMetricSamples([]metrics.SampleContainer{
		testSampleContainer{
			samples: []metrics.Sample{sample},
		},
	})

	// Verify data in ClickHouse using polling instead of fixed sleep
	ctx := context.Background()
	verifyDB, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%s@%s/%s", testUsername, testPassword, endpoint, dbName))
	require.NoError(t, err)
	defer func() { require.NoError(t, verifyDB.Close()) }()

	require.Eventually(t, func() bool {
		var count int
		err := verifyDB.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count)
		return err == nil && count == 1
	}, 5*time.Second, 100*time.Millisecond, "row should appear within timeout")

	var metricName string
	var metricValue float64
	var tags map[string]string

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
