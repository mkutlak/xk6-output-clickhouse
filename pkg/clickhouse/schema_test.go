package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.k6.io/k6/metrics"
)

// Simple Schema Tests

func TestSimpleSchema_InvalidIdentifiers(t *testing.T) {
	t.Parallel()

	schema := SimpleSchema{}
	ctx := context.Background()

	tests := []struct {
		name          string
		database      string
		table         string
		errorContains string
	}{
		{
			name:          "database name with special characters",
			database:      "k6'; DROP TABLE samples; --",
			table:         "samples",
			errorContains: "invalid database name",
		},
		{
			name:          "table name with special characters",
			database:      "k6",
			table:         "samples'; DROP DATABASE k6; --",
			errorContains: "invalid table name",
		},
		{
			name:          "empty database name",
			database:      "",
			table:         "samples",
			errorContains: "invalid database name",
		},
		{
			name:          "empty table name",
			database:      "k6",
			table:         "",
			errorContains: "invalid table name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := schema.CreateSchema(ctx, &sql.DB{}, tt.database, tt.table)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}

func TestSimpleSchema_InsertQuery(t *testing.T) {
	t.Parallel()

	schema := SimpleSchema{}

	tests := []struct {
		name     string
		database string
		table    string
	}{
		{
			name:     "default configuration",
			database: "k6",
			table:    "samples",
		},
		{
			name:     "custom names",
			database: "production",
			table:    "metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query := schema.InsertQuery(tt.database, tt.table)

			assert.Contains(t, query, "INSERT INTO")
			assert.Contains(t, query, fmt.Sprintf("`%s`.`%s`", tt.database, tt.table))
			assert.Contains(t, query, "timestamp")
			assert.Contains(t, query, "metric")
			assert.Contains(t, query, "value")
			assert.Contains(t, query, "tags")
		})
	}
}

func TestConvertToSimple(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()

	tests := []struct {
		name        string
		setupSample func() metrics.Sample
		checkResult func(t *testing.T, ss simpleSample)
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
			checkResult: func(t *testing.T, ss simpleSample) {
				assert.Equal(t, "http_reqs", ss.Metric)
				assert.Equal(t, 123.45, ss.Value)
				assert.NotNil(t, ss.Tags, "Tags should not be nil")
				assert.Equal(t, 0, len(ss.Tags), "Tags should be empty")
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
			checkResult: func(t *testing.T, ss simpleSample) {
				assert.Equal(t, "http_req_duration", ss.Metric)
				assert.Equal(t, 234.56, ss.Value)
				assert.Equal(t, "GET", ss.Tags["method"])
				assert.Equal(t, "200", ss.Tags["status"])
				assert.Equal(t, "/api/users", ss.Tags["endpoint"])
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
			checkResult: func(t *testing.T, ss simpleSample) {
				assert.Equal(t, "errors", ss.Metric)
				assert.Equal(t, 0.0, ss.Value)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sample := tt.setupSample()
			result := convertToSimple(sample)

			tt.checkResult(t, result)
			assert.Equal(t, sample.Time, result.Timestamp)
		})
	}
}

func TestSimpleConverter_Convert(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()
	converter := SimpleConverter{}
	ctx := context.Background()

	metric := registry.MustNewMetric("http_reqs", metrics.Counter)
	tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
		"method": "GET",
		"status": "200",
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

	row, err := converter.Convert(ctx, sample)
	assert.NoError(t, err)
	assert.Len(t, row, 4)
	assert.Equal(t, now, row[0])
	assert.Equal(t, "http_reqs", row[1])
	assert.Equal(t, 1.0, row[2])

	tagsMap, ok := row[3].(map[string]string)
	assert.True(t, ok)
	assert.Equal(t, "GET", tagsMap["method"])
	assert.Equal(t, "200", tagsMap["status"])
}

// Compatible Schema Tests

func TestCompatibleSchema_InvalidIdentifiers(t *testing.T) {
	t.Parallel()

	schema := CompatibleSchema{}
	ctx := context.Background()

	tests := []struct {
		name          string
		database      string
		table         string
		errorContains string
	}{
		{
			name:          "database name with special characters",
			database:      "k6'; DROP TABLE samples; --",
			table:         "samples",
			errorContains: "invalid database name",
		},
		{
			name:          "table name with special characters",
			database:      "k6",
			table:         "samples'; DROP DATABASE k6; --",
			errorContains: "invalid table name",
		},
		{
			name:          "empty database name",
			database:      "",
			table:         "samples",
			errorContains: "invalid database name",
		},
		{
			name:          "empty table name",
			database:      "k6",
			table:         "",
			errorContains: "invalid table name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := schema.CreateSchema(ctx, &sql.DB{}, tt.database, tt.table)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}

func TestCompatibleSchema_InsertQuery(t *testing.T) {
	t.Parallel()

	schema := CompatibleSchema{}

	tests := []struct {
		name     string
		database string
		table    string
	}{
		{
			name:     "default configuration",
			database: "k6",
			table:    "samples",
		},
		{
			name:     "custom names",
			database: "production",
			table:    "metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			query := schema.InsertQuery(tt.database, tt.table)

			assert.Contains(t, query, "INSERT INTO")
			assert.Contains(t, query, fmt.Sprintf("`%s`.`%s`", tt.database, tt.table))
			assert.Contains(t, query, "timestamp")
			assert.Contains(t, query, "metric")
			assert.Contains(t, query, "metric_type")
			assert.Contains(t, query, "value")
			assert.Contains(t, query, "testid")
			assert.Contains(t, query, "release")
			assert.Contains(t, query, "scenario")
			assert.Contains(t, query, "build_id")
			assert.Contains(t, query, "extra_tags")
		})
	}
}

func TestConvertToCompatible(t *testing.T) {
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

		cs, err := convertToCompatible(sample, 12345)
		assert.NoError(t, err)
		assert.Equal(t, uint32(123), cs.BuildID)
		assert.Equal(t, uint16(200), cs.Status)
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

		_, err := convertToCompatible(sample, 12345)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse buildId")
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

		_, err := convertToCompatible(sample, 12345)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse status")
	})
}

func TestConvertToCompatibleEdgeCases(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()

	tests := []struct {
		name        string
		setupSample func() metrics.Sample
		checkResult func(t *testing.T, cs compatibleSample, err error)
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
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "default", cs.TestID)
				assert.Equal(t, "master", cs.Branch)
				assert.NotZero(t, cs.BuildID)
				assert.True(t, cs.ExpectedResponse)
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
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "run-123", cs.TestID)
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
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.False(t, cs.ExpectedResponse)
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
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "value1", cs.ExtraTags["custom1"])
				assert.Equal(t, "value2", cs.ExtraTags["custom2"])
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
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "jobs", cs.UIFeature)
				assert.NotContains(t, cs.ExtraTags, "uiFeature")
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
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "snake", cs.UIFeature)
				// camelCase falls through to extra_tags since snake_case was consumed
				assert.Equal(t, "camel", cs.ExtraTags["uiFeature"])
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
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "my check name", cs.CheckName)
				assert.NotContains(t, cs.ExtraTags, "check")
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
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "fallback check", cs.CheckName)
				assert.NotContains(t, cs.ExtraTags, "check_name")
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
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, uint32(4294967295), cs.BuildID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sample := tt.setupSample()
			result, err := convertToCompatible(sample, 12345)

			tt.checkResult(t, result, err)
		})
	}
}

func TestCompatibleConverter_Convert(t *testing.T) {
	t.Parallel()

	registry := metrics.NewRegistry()
	converter := CompatibleConverter{}
	ctx := context.Background()

	t.Run("convert returns correct row format", func(t *testing.T) {
		t.Parallel()

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

		row, err := converter.Convert(ctx, sample)
		assert.NoError(t, err)
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
	})

	t.Run("convert error returns nil row", func(t *testing.T) {
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

		row, err := converter.Convert(ctx, sample)
		assert.Error(t, err)
		assert.Nil(t, row)
	})
}

func TestCompatibleSchema_ErrorWrapping(t *testing.T) {
	t.Parallel()

	t.Run("database creation error is properly wrapped", func(t *testing.T) {
		t.Parallel()

		baseErr := errors.New("connection timeout")
		wrappedErr := fmt.Errorf("failed to create database: %w", baseErr)

		assert.Contains(t, wrappedErr.Error(), "failed to create database")
		assert.ErrorIs(t, wrappedErr, baseErr)
	})

	t.Run("table creation error is properly wrapped", func(t *testing.T) {
		t.Parallel()

		baseErr := errors.New("syntax error")
		wrappedErr := fmt.Errorf("failed to create table: %w", baseErr)

		assert.Contains(t, wrappedErr.Error(), "failed to create table")
		assert.ErrorIs(t, wrappedErr, baseErr)
	})
}

// Benchmarks

func BenchmarkConvertToSimple(b *testing.B) {
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
		ss := convertToSimple(sample)
		_ = ss
	}
}

func BenchmarkConvertToCompatible(b *testing.B) {
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

	b.ResetTimer()
	for b.Loop() {
		cs, err := convertToCompatible(sample, 12345)
		if err != nil {
			b.Fatal(err)
		}
		_ = cs
	}
}
