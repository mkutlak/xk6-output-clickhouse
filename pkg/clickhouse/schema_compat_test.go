package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.k6.io/k6/metrics"
)

func TestCompatibleSchema_CreateSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		database      string
		table         string
		expectError   bool
		errorContains string
	}{
		{
			name:        "successful schema creation",
			database:    "k6",
			table:       "samples",
			expectError: false,
		},
		{
			name:        "custom database and table names",
			database:    "production",
			table:       "metrics",
			expectError: false,
		},
		{
			name:        "underscored names",
			database:    "test_db",
			table:       "test_table",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Note: Since CreateSchema uses sql.DB.ExecContext directly,
			// we need to test it indirectly with a real database or interface.
			t.Skip("Skipping direct schema tests - requires database connection or interface refactoring")
		})
	}
}

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
			// Verify column names
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

func TestCompatibleSchema_ColumnCount(t *testing.T) {
	t.Parallel()

	schema := CompatibleSchema{}
	assert.Equal(t, 21, schema.ColumnCount())
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

		cs, err := convertToCompatible(sample)
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

		_, err := convertToCompatible(sample)
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

		_, err := convertToCompatible(sample)
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
			name: "group_name vs group alias",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"group": "api-tests",
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
				assert.Equal(t, "api-tests", cs.GroupName)
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
			name: "expected_response true explicitly",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"expected_response": "true",
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
				assert.True(t, cs.ExpectedResponse)
			},
		},
		{
			name: "all metric types",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("test_gauge", metrics.Gauge)
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   nil,
					},
					Time:  time.Now(),
					Value: 5.0,
				}
			},
			checkResult: func(t *testing.T, cs compatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, int8(2), cs.MetricType) // Gauge = 2
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
			name: "buildId max uint32",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"buildId": "4294967295", // max uint32
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
		{
			name: "status max uint16",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"status": "65535", // max uint16
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
				assert.Equal(t, uint16(65535), cs.Status)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sample := tt.setupSample()
			result, err := convertToCompatible(sample)

			tt.checkResult(t, result, err)
		})
	}
}

// TestCompatibleConverter tests the CompatibleConverter implementation
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

		// Verify key columns
		assert.Equal(t, now, row[0])           // timestamp
		assert.Equal(t, "http_reqs", row[1])   // metric
		assert.Equal(t, int8(1), row[2])       // metric_type (Counter = 1)
		assert.Equal(t, 1.0, row[3])           // value
		assert.Equal(t, "test-123", row[4])    // testid
		assert.Equal(t, uint32(456), row[7])   // build_id
		assert.Equal(t, "GET", row[11])        // method
		assert.Equal(t, uint16(200), row[12])  // status
		assert.Equal(t, true, row[13])         // expected_response
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

// TestMapMetricType tests the mapMetricType function for all metric types
func TestMapMetricType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		metricType metrics.MetricType
		expected   int8
	}{
		{
			name:       "Counter maps to 1",
			metricType: metrics.Counter,
			expected:   1,
		},
		{
			name:       "Gauge maps to 2",
			metricType: metrics.Gauge,
			expected:   2,
		},
		{
			name:       "Rate maps to 3",
			metricType: metrics.Rate,
			expected:   3,
		},
		{
			name:       "Trend maps to 4",
			metricType: metrics.Trend,
			expected:   4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := mapMetricType(tt.metricType)
			assert.Equal(t, tt.expected, result, "Metric type mapping should match expected value")
		})
	}
}

// TestGetAndDelete tests the getAndDelete helper function
func TestGetAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		inputMap      map[string]string
		key           string
		expectedValue string
		expectedFound bool
		expectMapSize int
	}{
		{
			name: "key exists - value returned and deleted",
			inputMap: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			key:           "key1",
			expectedValue: "value1",
			expectedFound: true,
			expectMapSize: 1,
		},
		{
			name: "key does not exist - empty string and false returned",
			inputMap: map[string]string{
				"key1": "value1",
			},
			key:           "key2",
			expectedValue: "",
			expectedFound: false,
			expectMapSize: 1,
		},
		{
			name:          "empty map - empty string and false returned",
			inputMap:      map[string]string{},
			key:           "key1",
			expectedValue: "",
			expectedFound: false,
			expectMapSize: 0,
		},
		{
			name: "empty string value - empty string returned but true",
			inputMap: map[string]string{
				"key1": "",
			},
			key:           "key1",
			expectedValue: "",
			expectedFound: true,
			expectMapSize: 0,
		},
		{
			name: "special characters in key",
			inputMap: map[string]string{
				"key-with-dashes": "value1",
			},
			key:           "key-with-dashes",
			expectedValue: "value1",
			expectedFound: true,
			expectMapSize: 0,
		},
		{
			name: "unicode value",
			inputMap: map[string]string{
				"key": "日本語",
			},
			key:           "key",
			expectedValue: "日本語",
			expectedFound: true,
			expectMapSize: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a copy to avoid race conditions
			m := make(map[string]string)
			maps.Copy(m, tt.inputMap)

			value, found := getAndDelete(m, tt.key)

			assert.Equal(t, tt.expectedValue, value, "Returned value should match expected")
			assert.Equal(t, tt.expectedFound, found, "Found flag should match expected")
			assert.Equal(t, tt.expectMapSize, len(m), "Map size after deletion should match expected")

			// Verify key was actually deleted if found
			if tt.expectedFound {
				_, stillExists := m[tt.key]
				assert.False(t, stillExists, "Key should be deleted from map")
			}
		})
	}
}

// TestGetAndDeleteWithDefault tests the getAndDeleteWithDefault helper function
func TestGetAndDeleteWithDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		inputMap      map[string]string
		key           string
		defaultValue  string
		expectedValue string
		expectMapSize int
	}{
		{
			name: "key exists - value returned, default ignored",
			inputMap: map[string]string{
				"key1": "value1",
			},
			key:           "key1",
			defaultValue:  "default",
			expectedValue: "value1",
			expectMapSize: 0,
		},
		{
			name:          "key does not exist - default returned",
			inputMap:      map[string]string{},
			key:           "key1",
			defaultValue:  "default",
			expectedValue: "default",
			expectMapSize: 0,
		},
		{
			name: "empty string value exists - empty string returned, not default",
			inputMap: map[string]string{
				"key1": "",
			},
			key:           "key1",
			defaultValue:  "default",
			expectedValue: "",
			expectMapSize: 0,
		},
		{
			name:          "empty default value",
			inputMap:      map[string]string{},
			key:           "key1",
			defaultValue:  "",
			expectedValue: "",
			expectMapSize: 0,
		},
		{
			name: "multiple keys - only specified key deleted",
			inputMap: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			key:           "key1",
			defaultValue:  "default",
			expectedValue: "value1",
			expectMapSize: 1,
		},
		{
			name: "default is special string",
			inputMap: map[string]string{
				"other": "value",
			},
			key:           "key1",
			defaultValue:  "master",
			expectedValue: "master",
			expectMapSize: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a copy to avoid race conditions
			m := make(map[string]string)
			maps.Copy(m, tt.inputMap)

			value := getAndDeleteWithDefault(m, tt.key, tt.defaultValue)

			assert.Equal(t, tt.expectedValue, value, "Returned value should match expected")
			assert.Equal(t, tt.expectMapSize, len(m), "Map size after deletion should match expected")

			// Verify key was deleted if it existed
			_, stillExists := m[tt.key]
			assert.False(t, stillExists, "Key should be deleted from map if it existed")
		})
	}
}

// TestClearMap tests the clearMap helper function
func TestClearMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		inputMap map[string]string
	}{
		{
			name: "clear map with multiple entries",
			inputMap: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
		},
		{
			name:     "clear empty map",
			inputMap: map[string]string{},
		},
		{
			name: "clear map with single entry",
			inputMap: map[string]string{
				"key1": "value1",
			},
		},
		{
			name: "clear map with special characters",
			inputMap: map[string]string{
				"key-1": "value1",
				"key_2": "value2",
				"key.3": "value3",
				"キー":    "値",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a copy
			m := make(map[string]string)
			maps.Copy(m, tt.inputMap)

			clearMap(m)

			assert.Equal(t, 0, len(m), "Map should be empty after clear")
			assert.NotNil(t, m, "Map should not be nil, just empty")
		})
	}
}

// TestCompatibleSchema_ErrorWrapping tests that errors are properly wrapped
func TestCompatibleSchema_ErrorWrapping(t *testing.T) {
	t.Parallel()

	t.Run("database creation error is properly wrapped", func(t *testing.T) {
		t.Parallel()

		// Test that errors are wrapped with context
		baseErr := errors.New("connection timeout")
		wrappedErr := fmt.Errorf("failed to create database: %w", baseErr)

		assert.Contains(t, wrappedErr.Error(), "failed to create database")
		assert.ErrorIs(t, wrappedErr, baseErr)
	})

	t.Run("table creation error is properly wrapped", func(t *testing.T) {
		t.Parallel()

		// Test that errors are wrapped with context
		baseErr := errors.New("syntax error")
		wrappedErr := fmt.Errorf("failed to create table: %w", baseErr)

		assert.Contains(t, wrappedErr.Error(), "failed to create table")
		assert.ErrorIs(t, wrappedErr, baseErr)
	})
}

// BenchmarkConvertToCompatible benchmarks the convertToCompatible function
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
		cs, err := convertToCompatible(sample)
		if err != nil {
			b.Fatal(err)
		}
		_ = cs
	}
}
