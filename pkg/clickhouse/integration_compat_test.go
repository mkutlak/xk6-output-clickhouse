package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// TestIntegration_ClickHouse_CompatibleSchema exercises the 21-column compatible
// schema end-to-end against real ClickHouse: the typed/codec DDL, the Enum8
// metric_type mapping, tag extraction with type coercion (build_id/status), and
// the converter-applied defaults (testid='default', non-zero build_id) for a
// sample that carries no tags.
func TestIntegration_ClickHouse_CompatibleSchema(t *testing.T) {
	endpoint, cleanup := StartClickHouseContainer(t)
	defer cleanup()

	dbName := "k6"
	tableName := "samples_compat"

	CreateDatabase(t, endpoint, dbName)

	params := output.Params{
		Logger: newTestLogger(t),
		JSONConfig: mustMarshalJSON(map[string]any{
			"addr":         endpoint,
			"user":         testUsername,
			"password":     testPassword,
			"database":     dbName,
			"table":        tableName,
			"pushInterval": "100ms",
			"schemaMode":   "compatible",
		}),
	}

	out, err := New(params)
	require.NoError(t, err)

	err = out.Start()
	require.NoError(t, err)
	defer func() { require.NoError(t, out.Stop()) }()

	registry := metrics.NewRegistry()
	now := time.Now()

	// Sample 1: a Counter with known tags that map to typed columns (and require
	// numeric coercion for build_id/status).
	counter := registry.MustNewMetric("compat_counter", metrics.Counter)
	tagged := metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: counter,
			Tags: registry.RootTagSet().WithTagsFromMap(map[string]string{
				"buildId":           "42",
				"status":            "200",
				"testid":            "run-1",
				"method":            "GET",
				"expected_response": "true",
				"custom_label":      "kept", // unrecognized → extra_tags
			}),
		},
		Time:  now,
		Value: 7,
	}

	// Sample 2: a Trend with no tags — exercises converter defaults.
	trend := registry.MustNewMetric("compat_defaults", metrics.Trend)
	bare := metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: trend},
		Time:       now,
		Value:      1.5,
	}

	out.AddMetricSamples([]metrics.SampleContainer{
		&mockSampleContainer{samples: []metrics.Sample{tagged, bare}},
	})

	ctx := context.Background()
	verifyDB, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%s@%s/%s", testUsername, testPassword, endpoint, dbName))
	require.NoError(t, err)
	defer func() { require.NoError(t, verifyDB.Close()) }()

	require.Eventually(t, func() bool {
		var count int
		err := verifyDB.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count)
		return err == nil && count == 2
	}, 5*time.Second, 100*time.Millisecond, "both rows should appear within timeout")

	// Verify the tagged Counter row round-tripped through the typed columns.
	t.Run("tagged counter maps to typed columns", func(t *testing.T) {
		var (
			metricType       string
			testID           string
			buildID          uint32
			status           uint16
			method           string
			expectedResponse bool
			extraTags        map[string]string
		)
		err := verifyDB.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT metric_type, testid, build_id, status, method, expected_response, extra_tags FROM %s WHERE metric = 'compat_counter'",
			tableName,
		)).Scan(&metricType, &testID, &buildID, &status, &method, &expectedResponse, &extraTags)
		require.NoError(t, err)

		assert.Equal(t, "counter", metricType, "Counter should map to Enum8 'counter'")
		assert.Equal(t, "run-1", testID)
		assert.Equal(t, uint32(42), buildID, "buildId tag should coerce to UInt32")
		assert.Equal(t, uint16(200), status, "status tag should coerce to UInt16")
		assert.Equal(t, "GET", method)
		assert.True(t, expectedResponse)
		assert.Equal(t, "kept", extraTags["custom_label"], "unrecognized tags land in extra_tags")
	})

	// Verify the bare Trend row uses converter-applied defaults.
	t.Run("bare trend uses converter defaults", func(t *testing.T) {
		var (
			metricType string
			testID     string
			buildID    uint32
			status     uint16
		)
		err := verifyDB.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT metric_type, testid, build_id, status FROM %s WHERE metric = 'compat_defaults'",
			tableName,
		)).Scan(&metricType, &testID, &buildID, &status)
		require.NoError(t, err)

		assert.Equal(t, "trend", metricType, "Trend should map to Enum8 'trend'")
		assert.Equal(t, "default", testID, "missing testid defaults to 'default'")
		assert.Positive(t, buildID, "missing buildId defaults to a non-zero startup value")
		assert.Equal(t, uint16(0), status, "missing status defaults to 0")
	})
}
