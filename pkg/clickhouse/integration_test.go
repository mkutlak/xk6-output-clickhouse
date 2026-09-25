package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// miniSchema is a minimal custom Schema registered by the "custom schema"
// subtest below. It shows the whole extension surface a third party needs to
// add a schema: three columns, no tag extraction.
type miniSchema struct{}

func (miniSchema) CreateTable(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			timestamp DateTime64(3),
			metric    LowCardinality(String),
			value     Float64
		) ENGINE = MergeTree()
		ORDER BY (metric, timestamp)
	`, table)
}

func (miniSchema) InsertQuery(table string) string {
	return fmt.Sprintf("INSERT INTO %s (timestamp, metric, value) VALUES (?, ?, ?)", table)
}

func (miniSchema) Row(s metrics.Sample) ([]any, error) {
	return []any{s.Time, s.Metric.Name, s.Value}, nil
}

// TestIntegration runs the output end-to-end against a single shared
// ClickHouse container, one subtest per schema.
func TestIntegration(t *testing.T) {
	endpoint := startClickHouseContainer(t)

	t.Run("simple schema", func(t *testing.T) {
		const dbName, tableName = "itest_simple", "samples"

		arg := fmt.Sprintf("%s?database=%s&table=%s&user=%s&password=%s&pushInterval=50ms&schemaMode=simple",
			endpoint, dbName, tableName, testUsername, testPassword)
		out, err := New(output.Params{Logger: newTestLogger(t), ConfigArgument: arg})
		require.NoError(t, err)
		require.NoError(t, out.Start())

		sample := newSample(t, "test_metric", metrics.Trend, 123.45, map[string]string{"tag1": "value1"})
		out.AddMetricSamples([]metrics.SampleContainer{&mockSampleContainer{samples: []metrics.Sample{sample}}})
		require.NoError(t, out.Stop())

		db := openVerifyDB(t, endpoint, dbName)

		var metricName string
		var value float64
		var tags map[string]string
		require.NoError(t, db.QueryRowContext(context.Background(),
			fmt.Sprintf("SELECT metric, value, tags FROM %s", tableName),
		).Scan(&metricName, &value, &tags))

		assert.Equal(t, "test_metric", metricName)
		assert.Equal(t, 123.45, value)
		assert.Equal(t, "value1", tags["tag1"])
	})

	t.Run("compatible schema", func(t *testing.T) {
		const dbName, tableName = "itest_compat", "samples"

		dsn := fmt.Sprintf("clickhouse://%s:%s@%s/%s?schemaMode=compatible&table=%s",
			testUsername, testPassword, endpoint, dbName, tableName)
		out, err := New(output.Params{Logger: newTestLogger(t), ConfigArgument: dsn})
		require.NoError(t, err)
		require.NoError(t, out.Start())

		tagged := newSample(t, "compat_counter", metrics.Counter, 7, map[string]string{
			"buildId":      "42",
			"status":       "200",
			"testid":       "run-1",
			"method":       "GET",
			"check":        "status is 200",
			"custom_label": "kept",
		})
		bare := newSample(t, "compat_defaults", metrics.Trend, 1.5, nil)

		out.AddMetricSamples([]metrics.SampleContainer{&mockSampleContainer{samples: []metrics.Sample{tagged, bare}}})
		require.NoError(t, out.Stop())

		db := openVerifyDB(t, endpoint, dbName)
		ctx := context.Background()

		t.Run("tagged sample maps to typed columns", func(t *testing.T) {
			var (
				metricType string
				testID     string
				status     uint16
				method     string
				checkName  string
				extraTags  map[string]string
			)
			require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
				"SELECT metric_type, testid, status, method, check_name, extra_tags FROM %s WHERE metric = 'compat_counter'",
				tableName,
			)).Scan(&metricType, &testID, &status, &method, &checkName, &extraTags))

			assert.Equal(t, "counter", metricType)
			assert.Equal(t, "run-1", testID)
			assert.Equal(t, uint16(200), status)
			assert.Equal(t, "GET", method)
			assert.Equal(t, "status is 200", checkName)
			assert.Equal(t, "kept", extraTags["custom_label"])
		})

		t.Run("bare sample uses Row defaults", func(t *testing.T) {
			var testID string
			var status uint16
			require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
				"SELECT testid, status FROM %s WHERE metric = 'compat_defaults'", tableName,
			)).Scan(&testID, &status))

			assert.Equal(t, "default", testID)
			assert.Equal(t, uint16(0), status)
		})
	})

	t.Run("custom schema", func(t *testing.T) {
		RegisterSchema("itest_custom", miniSchema{})
		const dbName, tableName = "itest_custom", "samples"

		out, err := New(output.Params{
			Logger: newTestLogger(t),
			JSONConfig: mustMarshalJSON(map[string]any{
				"addr":       endpoint,
				"user":       testUsername,
				"password":   testPassword,
				"database":   dbName,
				"table":      tableName,
				"schemaMode": "itest_custom",
			}),
		})
		require.NoError(t, err)
		require.NoError(t, out.Start())

		sample := newSample(t, "custom_metric", metrics.Gauge, 9.5, nil)
		out.AddMetricSamples([]metrics.SampleContainer{&mockSampleContainer{samples: []metrics.Sample{sample}}})
		require.NoError(t, out.Stop())

		db := openVerifyDB(t, endpoint, dbName)

		var metricName string
		var value float64
		require.NoError(t, db.QueryRowContext(context.Background(),
			fmt.Sprintf("SELECT metric, value FROM %s", tableName),
		).Scan(&metricName, &value))

		assert.Equal(t, "custom_metric", metricName)
		assert.Equal(t, 9.5, value)
	})
}

// openVerifyDB opens a connection to dbName on endpoint for assertions.
func openVerifyDB(t *testing.T, endpoint, dbName string) *sql.DB {
	t.Helper()
	db, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%s@%s/%s", testUsername, testPassword, endpoint, dbName))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}
