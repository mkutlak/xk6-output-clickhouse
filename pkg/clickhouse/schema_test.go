package clickhouse

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
)

// compatColumns lists the compatible schema's columns in InsertQuery order.
var compatColumns = []string{
	"timestamp", "metric", "metric_type", "value",
	"testid", "release", "scenario", "build_id", "version", "branch",
	"name", "method", "status", "expected_response", "error_code",
	"rating", "resource_type", "ui_feature", "check_name", "group_name",
	"extra_tags",
}

// compatCols maps a compatible-schema row onto its column names, so
// assertions read by name instead of by position.
func compatCols(t *testing.T, row []any) map[string]any {
	t.Helper()
	require.Len(t, row, len(compatColumns))

	cols := make(map[string]any, len(row))
	for i, name := range compatColumns {
		cols[name] = row[i]
	}
	return cols
}

// TestSchema_Generic checks every registered schema's DDL, INSERT statement
// and Row conversion satisfy the Schema contract, independent of any
// schema-specific column layout.
func TestSchema_Generic(t *testing.T) {
	t.Parallel()

	table := escapeIdentifier("k6") + "." + escapeIdentifier("samples")

	for _, name := range availableSchemas() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			schema, err := getSchema(name)
			require.NoError(t, err)

			ddl := schema.CreateTable(table)
			assert.Contains(t, ddl, "IF NOT EXISTS")
			assert.Contains(t, ddl, table)

			insert := schema.InsertQuery(table)
			assert.Contains(t, insert, table)

			sample := newSample(t, "http_reqs", metrics.Counter, 1.0, nil)
			row, err := schema.Row(sample)
			require.NoError(t, err)
			assert.Len(t, row, strings.Count(insert, "?"))
		})
	}
}

func TestSimpleSchema_Row(t *testing.T) {
	t.Parallel()

	t.Run("nil tags become an empty map", func(t *testing.T) {
		t.Parallel()

		sample := newSample(t, "http_reqs", metrics.Counter, 123.45, nil)

		row, err := simpleSchema{}.Row(sample)
		require.NoError(t, err)
		assert.Equal(t, sample.Time, row[0])
		assert.Equal(t, "http_reqs", row[1])
		assert.Equal(t, 123.45, row[2])
		assert.Equal(t, map[string]string{}, row[3])
	})

	t.Run("tags are copied as-is", func(t *testing.T) {
		t.Parallel()

		tags := map[string]string{"method": "GET", "status": "200", "endpoint": "/api/users"}
		sample := newSample(t, "http_req_duration", metrics.Trend, 234.56, tags)

		row, err := simpleSchema{}.Row(sample)
		require.NoError(t, err)
		assert.Equal(t, tags, row[3])
	})

	t.Run("Row does not mutate the sample's TagSet", func(t *testing.T) {
		t.Parallel()

		tags := map[string]string{"method": "GET"}
		sample := newSample(t, "http_reqs", metrics.Counter, 1.0, tags)

		row, err := simpleSchema{}.Row(sample)
		require.NoError(t, err)

		got, ok := row[3].(map[string]string)
		require.True(t, ok)
		got["method"] = "mutated"
		got["extra"] = "added"

		assert.Equal(t, tags, sample.Tags.Map())
	})
}

func TestCompatSchema_Row(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		metricType metrics.MetricType
		tags       map[string]string
		wantErr    string
		check      func(t *testing.T, cols map[string]any)
	}{
		{
			name:       "defaults when no tags are set",
			metricType: metrics.Counter,
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "default", cols["testid"])
				assert.Equal(t, "master", cols["branch"])
				assert.Equal(t, uint32(12345), cols["build_id"])
				assert.Equal(t, true, cols["expected_response"])
				assert.Equal(t, uint16(0), cols["status"])
				assert.Equal(t, int8(1), cols["metric_type"])
				assert.Equal(t, map[string]string{}, cols["extra_tags"])
			},
		},
		{
			name:       "valid buildId and status parse",
			metricType: metrics.Counter,
			tags:       map[string]string{"buildId": "123", "status": "200"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, uint32(123), cols["build_id"])
				assert.Equal(t, uint16(200), cols["status"])
			},
		},
		{
			name:       "buildId zero falls back to default",
			metricType: metrics.Counter,
			tags:       map[string]string{"buildId": "0"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, uint32(12345), cols["build_id"])
			},
		},
		{
			name:       "invalid buildId errors",
			metricType: metrics.Counter,
			tags:       map[string]string{"buildId": "invalid"},
			wantErr:    "failed to parse buildId",
		},
		{
			name:       "empty buildId errors",
			metricType: metrics.Counter,
			tags:       map[string]string{"buildId": ""},
			wantErr:    "failed to parse buildId",
		},
		{
			name:       "invalid status errors",
			metricType: metrics.Counter,
			tags:       map[string]string{"status": "invalid"},
			wantErr:    "failed to parse status",
		},
		{
			name:       "empty status errors",
			metricType: metrics.Counter,
			tags:       map[string]string{"status": ""},
			wantErr:    "failed to parse status",
		},
		{
			name:       "expected_response false",
			metricType: metrics.Counter,
			tags:       map[string]string{"expected_response": "false"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, false, cols["expected_response"])
			},
		},
		{
			name:       "expected_response is lenient about anything but true",
			metricType: metrics.Counter,
			tags:       map[string]string{"expected_response": "yes"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, false, cols["expected_response"])
			},
		},
		{
			name:       "metric type gauge",
			metricType: metrics.Gauge,
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, int8(2), cols["metric_type"])
			},
		},
		{
			name:       "metric type rate",
			metricType: metrics.Rate,
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, int8(3), cols["metric_type"])
			},
		},
		{
			name:       "metric type trend",
			metricType: metrics.Trend,
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, int8(4), cols["metric_type"])
			},
		},
		{
			name:       "unknown tags land in extra_tags",
			metricType: metrics.Counter,
			tags:       map[string]string{"custom1": "value1", "custom2": "value2"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, map[string]string{"custom1": "value1", "custom2": "value2"}, cols["extra_tags"])
			},
		},
		{
			name:       "testid wins over test_run_id, loser kept in extra_tags",
			metricType: metrics.Counter,
			tags:       map[string]string{"testid": "direct", "test_run_id": "aliased"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "direct", cols["testid"])
				assert.Equal(t, map[string]string{"test_run_id": "aliased"}, cols["extra_tags"])
			},
		},
		{
			name:       "test_run_id used when testid is absent",
			metricType: metrics.Counter,
			tags:       map[string]string{"test_run_id": "run-123"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "run-123", cols["testid"])
				assert.Equal(t, map[string]string{}, cols["extra_tags"])
			},
		},
		{
			name:       "ui_feature wins over uiFeature, loser kept in extra_tags",
			metricType: metrics.Gauge,
			tags:       map[string]string{"ui_feature": "snake", "uiFeature": "camel"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "snake", cols["ui_feature"])
				assert.Equal(t, map[string]string{"uiFeature": "camel"}, cols["extra_tags"])
			},
		},
		{
			name:       "uiFeature used when ui_feature is absent",
			metricType: metrics.Gauge,
			tags:       map[string]string{"uiFeature": "jobs"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "jobs", cols["ui_feature"])
				assert.Equal(t, map[string]string{}, cols["extra_tags"])
			},
		},
		{
			name:       "check wins over check_name, loser kept in extra_tags",
			metricType: metrics.Rate,
			tags:       map[string]string{"check": "native", "check_name": "alias"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "native", cols["check_name"])
				assert.Equal(t, map[string]string{"check_name": "alias"}, cols["extra_tags"])
			},
		},
		{
			name:       "check_name used when check is absent",
			metricType: metrics.Rate,
			tags:       map[string]string{"check_name": "fallback check"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "fallback check", cols["check_name"])
				assert.Equal(t, map[string]string{}, cols["extra_tags"])
			},
		},
		{
			name:       "group_name wins over group, loser kept in extra_tags",
			metricType: metrics.Counter,
			tags:       map[string]string{"group_name": "explicit", "group": "::g"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "explicit", cols["group_name"])
				assert.Equal(t, map[string]string{"group": "::g"}, cols["extra_tags"])
			},
		},
		{
			name:       "group used when group_name is absent",
			metricType: metrics.Counter,
			tags:       map[string]string{"group": "::g"},
			check: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "::g", cols["group_name"])
				assert.Equal(t, map[string]string{}, cols["extra_tags"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sample := newSample(t, "http_reqs", tt.metricType, 1.0, tt.tags)
			row, err := compatSchema{defaultBuildID: 12345}.Row(sample)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, row)
				return
			}

			require.NoError(t, err)
			tt.check(t, compatCols(t, row))
		})
	}
}

// Benchmarks

func BenchmarkSimpleSchema_Row(b *testing.B) {
	sample := newSample(b, "http_req_duration", metrics.Trend, 123.45, map[string]string{
		"method": "GET",
		"status": "200",
	})

	for b.Loop() {
		if _, err := (simpleSchema{}).Row(sample); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompatSchema_Row(b *testing.B) {
	sample := newSample(b, "http_reqs", metrics.Counter, 1.0, map[string]string{
		"method": "GET",
		"status": "200",
		"testid": "test-123",
	})

	schema := compatSchema{defaultBuildID: 12345}

	for b.Loop() {
		if _, err := schema.Row(sample); err != nil {
			b.Fatal(err)
		}
	}
}
