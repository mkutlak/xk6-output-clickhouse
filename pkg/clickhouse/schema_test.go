package clickhouse

import (
	"fmt"
	"strings"
	"testing"
	"time"

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

// compatRow converts sample with the compatible schema (default build ID 12345)
// and returns the row keyed by column name.
func compatRow(t *testing.T, sample metrics.Sample) map[string]any {
	t.Helper()

	row, err := compatSchema{defaultBuildID: 12345}.Row(sample)
	require.NoError(t, err)
	require.Len(t, row, len(compatColumns))

	cols := make(map[string]any, len(row))
	for i, name := range compatColumns {
		cols[name] = row[i]
	}
	return cols
}

// TestSchema_Queries checks both schemas' DDL and INSERT statements target the
// given table, and that the INSERT column order matches the Row layout.
func TestSchema_Queries(t *testing.T) {
	t.Parallel()

	schemas := []struct {
		name    string
		schema  Schema
		columns []string
	}{
		{"simple", simpleSchema{}, []string{"timestamp", "metric", "value", "tags"}},
		{"compatible", compatSchema{}, compatColumns},
	}

	nameCases := []struct {
		name     string
		database string
		table    string
	}{
		{"default configuration", "k6", "samples"},
		{"custom names", "production", "metrics"},
	}

	for _, s := range schemas {
		for _, tt := range nameCases {
			t.Run(s.name+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				table := escapeIdentifier(tt.database) + "." + escapeIdentifier(tt.table)
				want := fmt.Sprintf("`%s`.`%s`", tt.database, tt.table)

				ddl := s.schema.CreateTable(table)
				assert.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS "+want)
				assert.Contains(t, ddl, "DateTime64(3")

				// Collapse whitespace so the multi-line column list compares as one string.
				query := strings.Join(strings.Fields(s.schema.InsertQuery(table)), " ")
				assert.Contains(t, query, "INSERT INTO "+want)
				assert.Contains(t, query, strings.Join(s.columns, ", "))
				assert.Equal(t, len(s.columns), strings.Count(query, "?"))
			})
		}
	}
}

func TestSimpleSchema_Row(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()

	tests := []struct {
		name        string
		setupSample func() metrics.Sample
		checkResult func(t *testing.T, row []any)
	}{
		{
			name: "sample with nil tags",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   nil,
					},
					Time:  time.Now(),
					Value: 123.45,
				}
			},
			checkResult: func(t *testing.T, row []any) {
				assert.Equal(t, "http_reqs", row[1])
				assert.Equal(t, 123.45, row[2])
				assert.Equal(t, map[string]string{}, row[3], "Tags should be an empty, non-nil map")
			},
		},
		{
			name: "sample with multiple tags",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_req_duration", metrics.Trend)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"method":   "GET",
					"status":   "200",
					"endpoint": "/api/users",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 234.56,
				}
			},
			checkResult: func(t *testing.T, row []any) {
				assert.Equal(t, "http_req_duration", row[1])
				assert.Equal(t, 234.56, row[2])
				assert.Equal(t, map[string]string{
					"method":   "GET",
					"status":   "200",
					"endpoint": "/api/users",
				}, row[3])
			},
		},
		{
			name: "sample with zero value",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("errors", metrics.Rate)
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   nil,
					},
					Time:  time.Now(),
					Value: 0.0,
				}
			},
			checkResult: func(t *testing.T, row []any) {
				assert.Equal(t, "errors", row[1])
				assert.Equal(t, 0.0, row[2])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sample := tt.setupSample()
			row, err := simpleSchema{}.Row(sample)
			require.NoError(t, err)
			require.Len(t, row, 4)

			assert.Equal(t, sample.Time, row[0])
			tt.checkResult(t, row)
		})
	}
}

func TestCompatSchema_Row(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()

	t.Run("valid sample", func(t *testing.T) {
		t.Parallel()

		metric := registry.MustNewMetric("http_reqs", metrics.Counter)
		tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
			"buildId": "123",
			"status":  "200",
		})
		sample := metrics.Sample{
			TimeSeries: metrics.TimeSeries{
				Metric: metric,
				Tags:   tags,
			},
			Time:  time.Now(),
			Value: 1.0,
		}

		cols := compatRow(t, sample)
		assert.Equal(t, uint32(123), cols["build_id"])
		assert.Equal(t, uint16(200), cols["status"])
	})

	t.Run("invalid buildId", func(t *testing.T) {
		t.Parallel()

		metric := registry.MustNewMetric("http_reqs", metrics.Counter)
		tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
			"buildId": "invalid",
		})
		sample := metrics.Sample{
			TimeSeries: metrics.TimeSeries{
				Metric: metric,
				Tags:   tags,
			},
			Time:  time.Now(),
			Value: 1.0,
		}

		row, err := compatSchema{defaultBuildID: 12345}.Row(sample)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse buildId")
		assert.Contains(t, err.Error(), `"invalid"`)
		assert.Nil(t, row)
	})

	t.Run("invalid status", func(t *testing.T) {
		t.Parallel()

		metric := registry.MustNewMetric("http_reqs", metrics.Counter)
		tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
			"status": "invalid",
		})
		sample := metrics.Sample{
			TimeSeries: metrics.TimeSeries{
				Metric: metric,
				Tags:   tags,
			},
			Time:  time.Now(),
			Value: 1.0,
		}

		row, err := compatSchema{defaultBuildID: 12345}.Row(sample)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse status")
		assert.Nil(t, row)
	})

	t.Run("empty typed tags fail to parse", func(t *testing.T) {
		t.Parallel()

		for _, key := range []string{"buildId", "status"} {
			sample := metrics.Sample{
				TimeSeries: metrics.TimeSeries{
					Metric: registry.MustNewMetric("http_reqs", metrics.Counter),
					Tags:   registry.RootTagSet().WithTagsFromMap(map[string]string{key: ""}),
				},
				Time:  time.Now(),
				Value: 1.0,
			}

			_, err := compatSchema{defaultBuildID: 12345}.Row(sample)
			assert.ErrorContains(t, err, "failed to parse "+key)
		}
	})
}

func TestCompatSchema_RowEdgeCases(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()
	withTags := func(tags map[string]string) func() metrics.Sample {
		return func() metrics.Sample {
			return metrics.Sample{
				TimeSeries: metrics.TimeSeries{
					Metric: registry.MustNewMetric("http_reqs", metrics.Counter),
					Tags:   registry.RootTagSet().WithTagsFromMap(tags),
				},
				Time:  time.Now(),
				Value: 1.0,
			}
		}
	}

	tests := []struct {
		name        string
		setupSample func() metrics.Sample
		checkResult func(t *testing.T, cols map[string]any)
	}{
		{
			name: "sample with no tags - uses defaults",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   nil,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			checkResult: func(t *testing.T, cols map[string]any) {
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
			name: "testid vs test_run_id alias",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"test_run_id": "run-123",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "run-123", cols["testid"])
			},
		},
		{
			name: "expected_response false",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"expected_response": "false",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, false, cols["expected_response"])
			},
		},
		{
			name: "extra tags preserved",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"custom1": "value1",
					"custom2": "value2",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, map[string]string{"custom1": "value1", "custom2": "value2"}, cols["extra_tags"])
			},
		},
		{
			name: "uiFeature camelCase alias",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("browser_web_vital_fcp", metrics.Gauge)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"uiFeature": "jobs",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "jobs", cols["ui_feature"])
				assert.NotContains(t, cols["extra_tags"], "uiFeature")
			},
		},
		{
			name: "ui_feature snake_case takes precedence over uiFeature",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("browser_web_vital_fcp", metrics.Gauge)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"ui_feature": "snake",
					"uiFeature":  "camel",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "snake", cols["ui_feature"])
				// camelCase falls through to extra_tags since snake_case was consumed
				assert.Equal(t, map[string]string{"uiFeature": "camel"}, cols["extra_tags"])
			},
		},
		{
			name: "check tag mapped to check_name",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("checks", metrics.Rate)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"check": "my check name",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "my check name", cols["check_name"])
				assert.NotContains(t, cols["extra_tags"], "check")
			},
		},
		{
			name: "check_name alias fallback",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("checks", metrics.Rate)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"check_name": "fallback check",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "fallback check", cols["check_name"])
				assert.NotContains(t, cols["extra_tags"], "check_name")
			},
		},
		{
			name: "buildId max uint32",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"buildId": "4294967295",
				})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 1.0,
				}
			},
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, uint32(4294967295), cols["build_id"])
			},
		},
		{
			name:        "buildId zero falls back to default",
			setupSample: withTags(map[string]string{"buildId": "0"}),
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, uint32(12345), cols["build_id"])
				assert.Equal(t, map[string]string{}, cols["extra_tags"])
			},
		},
		{
			name:        "expected_response other than true is false",
			setupSample: withTags(map[string]string{"expected_response": "yes"}),
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, false, cols["expected_response"])
			},
		},
		{
			name:        "check takes precedence over check_name",
			setupSample: withTags(map[string]string{"check": "native", "check_name": "alias"}),
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "native", cols["check_name"])
				assert.Equal(t, map[string]string{"check_name": "alias"}, cols["extra_tags"])
			},
		},
		{
			name:        "group_name takes precedence over group",
			setupSample: withTags(map[string]string{"group_name": "explicit", "group": "::g"}),
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "explicit", cols["group_name"])
				assert.Equal(t, map[string]string{"group": "::g"}, cols["extra_tags"])
			},
		},
		{
			name:        "group alias fallback",
			setupSample: withTags(map[string]string{"group": "::g"}),
			checkResult: func(t *testing.T, cols map[string]any) {
				assert.Equal(t, "::g", cols["group_name"])
				assert.Equal(t, map[string]string{}, cols["extra_tags"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.checkResult(t, compatRow(t, tt.setupSample()))
		})
	}
}

func TestCompatSchema_RowLayout(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()
	metric := registry.MustNewMetric("http_reqs", metrics.Counter)
	tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
		"method":  "GET",
		"status":  "200",
		"testid":  "test-123",
		"buildId": "456",
	})
	now := time.Now()
	sample := metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: metric,
			Tags:   tags,
		},
		Time:  now,
		Value: 1.0,
	}

	row, err := compatSchema{}.Row(sample)
	require.NoError(t, err)
	assert.Len(t, row, 21)

	assert.Equal(t, now, row[0])
	assert.Equal(t, "http_reqs", row[1])
	assert.Equal(t, int8(1), row[2])
	assert.Equal(t, 1.0, row[3])
	assert.Equal(t, "test-123", row[4])
	assert.Equal(t, uint32(456), row[7])
	assert.Equal(t, "GET", row[11])
	assert.Equal(t, uint16(200), row[12])
	assert.Equal(t, true, row[13])
}

// Benchmarks

func BenchmarkSimpleSchema_Row(b *testing.B) {
	registry := metrics.NewRegistry()
	metric := registry.MustNewMetric("http_req_duration", metrics.Trend)
	tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
		"method": "GET",
		"status": "200",
	})

	sample := metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: metric,
			Tags:   tags,
		},
		Time:  time.Now(),
		Value: 123.45,
	}

	b.ResetTimer()
	for b.Loop() {
		if _, err := (simpleSchema{}).Row(sample); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompatSchema_Row(b *testing.B) {
	registry := metrics.NewRegistry()
	metric := registry.MustNewMetric("http_reqs", metrics.Counter)
	tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
		"method": "GET",
		"status": "200",
		"testid": "test-123",
	})

	sample := metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: metric,
			Tags:   tags,
		},
		Time:  time.Now(),
		Value: 1.0,
	}

	schema := compatSchema{defaultBuildID: 12345}

	b.ResetTimer()
	for b.Loop() {
		if _, err := schema.Row(sample); err != nil {
			b.Fatal(err)
		}
	}
}
