package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.k6.io/k6/metrics"
)

func TestConvertToCompatible(t *testing.T) {
	ctx := context.Background()
	registry := metrics.NewRegistry()

	t.Run("valid sample", func(t *testing.T) {
		metric := registry.MustNewMetric("http_reqs", metrics.Counter)
		tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
			"build_id": "123",
			"status":   "200",
		})
		sample := metrics.Sample{
			TimeSeries: metrics.TimeSeries{
				Metric: metric,
				Tags:   tags,
			},
			Time:  time.Now(),
			Value: 1.0,
		}

		cs, err := ConvertToCompatible(ctx, sample)
		assert.NoError(t, err)
		assert.Equal(t, uint32(123), cs.BuildID)
		assert.Equal(t, uint16(200), cs.Status)
	})

	t.Run("invalid build_id", func(t *testing.T) {
		metric := registry.MustNewMetric("http_reqs", metrics.Counter)
		tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
			"build_id": "invalid",
		})
		sample := metrics.Sample{
			TimeSeries: metrics.TimeSeries{
				Metric: metric,
				Tags:   tags,
			},
			Time:  time.Now(),
			Value: 1.0,
		}

		_, err := ConvertToCompatible(ctx, sample)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse build_id")
	})

	t.Run("invalid status", func(t *testing.T) {
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

		_, err := ConvertToCompatible(ctx, sample)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse status")
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
			for k, v := range tt.inputMap {
				m[k] = v
			}

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
			for k, v := range tt.inputMap {
				m[k] = v
			}

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
			for k, v := range tt.inputMap {
				m[k] = v
			}

			clearMap(m)

			assert.Equal(t, 0, len(m), "Map should be empty after clear")
			assert.NotNil(t, m, "Map should not be nil, just empty")
		})
	}
}

// TestConvertToSimpleEdgeCases tests edge cases for ConvertToSimple
func TestConvertToSimpleEdgeCases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	registry := metrics.NewRegistry()

	tests := []struct {
		name        string
		setupSample func() metrics.Sample
		checkResult func(t *testing.T, ss SimpleSample)
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
			checkResult: func(t *testing.T, ss SimpleSample) {
				assert.Equal(t, "http_reqs", ss.Metric)
				assert.Equal(t, 123.45, ss.Value)
				assert.NotNil(t, ss.Tags, "Tags should not be nil")
				assert.Equal(t, 0, len(ss.Tags), "Tags should be empty")
			},
		},
		{
			name: "sample with empty tags",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("vus", metrics.Gauge)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{})
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: 10,
				}
			},
			checkResult: func(t *testing.T, ss SimpleSample) {
				assert.Equal(t, "vus", ss.Metric)
				assert.Equal(t, float64(10), ss.Value)
				assert.NotNil(t, ss.Tags)
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
					"region":   "us-east-1",
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
			checkResult: func(t *testing.T, ss SimpleSample) {
				assert.Equal(t, "http_req_duration", ss.Metric)
				assert.Equal(t, 234.56, ss.Value)
				assert.Equal(t, "GET", ss.Tags["method"])
				assert.Equal(t, "200", ss.Tags["status"])
				assert.Equal(t, "/api/users", ss.Tags["endpoint"])
				assert.Equal(t, "us-east-1", ss.Tags["region"])
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
			checkResult: func(t *testing.T, ss SimpleSample) {
				assert.Equal(t, "errors", ss.Metric)
				assert.Equal(t, 0.0, ss.Value)
			},
		},
		{
			name: "sample with negative value",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("custom", metrics.Gauge)
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   nil,
					},
					Time:  time.Now(),
					Value: -42.5,
				}
			},
			checkResult: func(t *testing.T, ss SimpleSample) {
				assert.Equal(t, "custom", ss.Metric)
				assert.Equal(t, -42.5, ss.Value)
			},
		},
		{
			name: "sample with very large value",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("bytes", metrics.Counter)
				return metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   nil,
					},
					Time:  time.Now(),
					Value: 9999999999.999999,
				}
			},
			checkResult: func(t *testing.T, ss SimpleSample) {
				assert.Equal(t, "bytes", ss.Metric)
				assert.Equal(t, 9999999999.999999, ss.Value)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sample := tt.setupSample()
			result := ConvertToSimple(ctx, sample)

			tt.checkResult(t, result)

			// Verify timestamp is preserved
			assert.Equal(t, sample.Time, result.Timestamp)
		})
	}
}

// TestConvertToCompatibleEdgeCases tests edge cases for ConvertToCompatible
func TestConvertToCompatibleEdgeCases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	registry := metrics.NewRegistry()

	tests := []struct {
		name        string
		setupSample func() metrics.Sample
		checkResult func(t *testing.T, cs CompatibleSample, err error)
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
			checkResult: func(t *testing.T, cs CompatibleSample, err error) {
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
			checkResult: func(t *testing.T, cs CompatibleSample, err error) {
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
			checkResult: func(t *testing.T, cs CompatibleSample, err error) {
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
			checkResult: func(t *testing.T, cs CompatibleSample, err error) {
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
			checkResult: func(t *testing.T, cs CompatibleSample, err error) {
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
			checkResult: func(t *testing.T, cs CompatibleSample, err error) {
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
			checkResult: func(t *testing.T, cs CompatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, "value1", cs.ExtraTags["custom1"])
				assert.Equal(t, "value2", cs.ExtraTags["custom2"])
			},
		},
		{
			name: "build_id overflow - max uint32",
			setupSample: func() metrics.Sample {
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"build_id": "4294967295", // max uint32
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
			checkResult: func(t *testing.T, cs CompatibleSample, err error) {
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
			checkResult: func(t *testing.T, cs CompatibleSample, err error) {
				assert.NoError(t, err)
				assert.Equal(t, uint16(65535), cs.Status)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sample := tt.setupSample()
			result, err := ConvertToCompatible(ctx, sample)

			tt.checkResult(t, result, err)
		})
	}
}

// BenchmarkConvertToSimple benchmarks the ConvertToSimple function
func BenchmarkConvertToSimple(b *testing.B) {
	ctx := context.Background()
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
	for i := 0; i < b.N; i++ {
		ss := ConvertToSimple(ctx, sample)
		_ = ss
	}
}

// BenchmarkConvertToCompatible benchmarks the ConvertToCompatible function
func BenchmarkConvertToCompatible(b *testing.B) {
	ctx := context.Background()
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
	for i := 0; i < b.N; i++ {
		cs, err := ConvertToCompatible(ctx, sample)
		if err != nil {
			b.Fatal(err)
		}
		_ = cs
	}
}
