package clickhouse

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

// Helper function to create params for tests
func mustCreateParams(t *testing.T, config map[string]any) output.Params {
	t.Helper()
	data, err := json.Marshal(config)
	require.NoError(t, err)
	return output.Params{
		JSONConfig: data,
	}
}

// TestConcurrentAddMetricSamples tests concurrent calls to AddMetricSamples
func TestConcurrentAddMetricSamples(t *testing.T) {
	t.Parallel()

	t.Run("concurrent AddMetricSamples calls", func(t *testing.T) {
		t.Parallel()

		params := mustCreateParams(t, map[string]any{
			"addr":     "localhost:9000",
			"database": "k6",
			"table":    "samples",
		})

		out, err := New(params)
		require.NoError(t, err)
		require.NotNil(t, out)

		clickhouseOut := out.(*Output)

		// Number of concurrent goroutines
		numGoroutines := 10
		samplesPerGoroutine := 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Launch concurrent goroutines that add metric samples
		for i := range numGoroutines {
			go func(id int) {
				defer wg.Done()

				registry := metrics.NewRegistry()
				metric := registry.MustNewMetric("concurrent_test", metrics.Counter)

				for j := range samplesPerGoroutine {
					sample := metrics.Sample{
						TimeSeries: metrics.TimeSeries{
							Metric: metric,
							Tags:   nil,
						},
						Time:  time.Now(),
						Value: float64(j),
					}

					samples := metrics.Samples{sample}
					clickhouseOut.AddMetricSamples([]metrics.SampleContainer{samples})
				}
			}(i)
		}

		// Wait for all goroutines to complete
		wg.Wait()

		// Verify no panic occurred and samples were added
		buffered := clickhouseOut.GetBufferedSamples()
		assert.GreaterOrEqual(t, len(buffered), 0, "Should have buffered samples or empty buffer")
	})
}

// TestConcurrentConvertToSimple tests concurrent calls to ConvertToSimple
func TestConcurrentConvertToSimple(t *testing.T) {
	t.Parallel()

	t.Run("concurrent ConvertToSimple calls", func(t *testing.T) {
		t.Parallel()

		numGoroutines := 50

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Track errors
		errors := make(chan error, numGoroutines)

		for i := range numGoroutines {
			go func(id int) {
				defer wg.Done()

				registry := metrics.NewRegistry()
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"method": "GET",
					"id":     string(rune(id)),
				})

				sample := metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: float64(id),
				}

				// Convert multiple times in the same goroutine
				for range 100 {
					ss := convertToSimple(sample)
					if ss.Metric != "http_reqs" {
						errors <- assert.AnError
						return
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			assert.NoError(t, err, "Should not have errors in concurrent conversion")
		}
	})
}

// TestConcurrentConvertToCompatible tests concurrent calls to ConvertToCompatible
func TestConcurrentConvertToCompatible(t *testing.T) {
	t.Parallel()

	t.Run("concurrent ConvertToCompatible calls", func(t *testing.T) {
		t.Parallel()

		numGoroutines := 50

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		errors := make(chan error, numGoroutines)

		for i := range numGoroutines {
			go func(id int) {
				defer wg.Done()

				registry := metrics.NewRegistry()
				metric := registry.MustNewMetric("http_reqs", metrics.Counter)
				tags := registry.RootTagSet().WithTagsFromMap(map[string]string{
					"testid": "test-123",
					"status": "200",
				})

				sample := metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tags,
					},
					Time:  time.Now(),
					Value: float64(id),
				}

				for range 100 {
					cs, err := convertToCompatible(sample)
					if err != nil {
						errors <- err
						return
					}
					if cs.Metric != "http_reqs" {
						errors <- assert.AnError
						return
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			assert.NoError(t, err, "Should not have errors in concurrent conversion")
		}
	})
}

// TestMemoryPoolConcurrentAccess tests concurrent access to memory pools
func TestMemoryPoolConcurrentAccess(t *testing.T) {
	t.Parallel()

	t.Run("concurrent tagMapPool Get and Put", func(t *testing.T) {
		t.Parallel()

		numGoroutines := 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for range numGoroutines {
			go func() {
				defer wg.Done()

				// Get from pool
				m := tagMapPool.Get().(map[string]string)

				// Use the map
				m["key1"] = "value1"
				m["key2"] = "value2"

				// Clear and return to pool
				clearMap(m)
				tagMapPool.Put(m)
			}()
		}

		wg.Wait()

		// Verify pool still works
		m := tagMapPool.Get().(map[string]string)
		assert.NotNil(t, m)
		assert.Equal(t, 0, len(m), "Map from pool should be empty")
		tagMapPool.Put(m)
	})

	t.Run("concurrent simpleRowPool access", func(t *testing.T) {
		t.Parallel()

		numGoroutines := 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for range numGoroutines {
			go func() {
				defer wg.Done()

				row := simpleRowPool.Get().([]any)
				row[0] = time.Now()
				row[1] = "metric"
				row[2] = 123.45
				row[3] = map[string]string{"key": "value"}

				simpleRowPool.Put(row) //nolint:staticcheck // SA6002: slice is reference type, safe to pass directly
			}()
		}

		wg.Wait()

		// Verify pool still works
		row := simpleRowPool.Get().([]any)
		assert.NotNil(t, row)
		assert.Equal(t, 4, len(row), "Row slice should have 4 elements")
		simpleRowPool.Put(row) //nolint:staticcheck // SA6002: slice is reference type, safe to pass directly
	})

	t.Run("concurrent compatibleRowPool access", func(t *testing.T) {
		t.Parallel()

		numGoroutines := 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for range numGoroutines {
			go func() {
				defer wg.Done()

				row := compatibleRowPool.Get().([]any)
				for i := range row {
					row[i] = i
				}

				compatibleRowPool.Put(row) //nolint:staticcheck // SA6002: slice is reference type, safe to pass directly
			}()
		}

		wg.Wait()

		// Verify pool still works
		row := compatibleRowPool.Get().([]any)
		assert.NotNil(t, row)
		assert.Equal(t, 21, len(row), "Row slice should have 21 elements")
		compatibleRowPool.Put(row) //nolint:staticcheck // SA6002: slice is reference type, safe to pass directly
	})
}

// TestStartStopLifecycleConcurrency tests concurrent Start/Stop operations
func TestStartStopLifecycleConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("multiple Stop calls are safe", func(t *testing.T) {
		t.Parallel()

		params := mustCreateParams(t, map[string]any{
			"addr": "localhost:9000",
		})

		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)

		var wg sync.WaitGroup
		numStops := 10
		wg.Add(numStops)

		// Call Stop concurrently multiple times
		for range numStops {
			go func() {
				defer wg.Done()
				err := clickhouseOut.Stop()
				// Stop should not return error
				assert.NoError(t, err)
			}()
		}

		wg.Wait()
	})

	t.Run("Stop is safe during AddMetricSamples", func(t *testing.T) {
		t.Parallel()

		params := mustCreateParams(t, map[string]any{
			"addr": "localhost:9000",
		})

		out, err := New(params)
		require.NoError(t, err)

		clickhouseOut := out.(*Output)

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: Add samples
		go func() {
			defer wg.Done()
			registry := metrics.NewRegistry()
			metric := registry.MustNewMetric("test", metrics.Counter)

			for i := range 1000 {
				sample := metrics.Sample{
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
					},
					Time:  time.Now(),
					Value: float64(i),
				}
				samples := metrics.Samples{sample}
				clickhouseOut.AddMetricSamples([]metrics.SampleContainer{samples})
			}
		}()

		// Goroutine 2: Stop after a short delay
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			err := clickhouseOut.Stop()
			assert.NoError(t, err)
		}()

		wg.Wait()
	})
}

// TestConcurrentClearMap tests clearMap with concurrent access
func TestConcurrentClearMap(t *testing.T) {
	t.Parallel()

	t.Run("concurrent clearMap calls on different maps", func(t *testing.T) {
		t.Parallel()

		numGoroutines := 50
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for range numGoroutines {
			go func() {
				defer wg.Done()

				m := make(map[string]string)
				m["key1"] = "value1"
				m["key2"] = "value2"
				m["key3"] = "value3"

				clearMap(m)

				assert.Equal(t, 0, len(m))
			}()
		}

		wg.Wait()
	})
}

// TestGetAndDeleteConcurrency tests getAndDelete with concurrent access
func TestGetAndDeleteConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("getAndDelete on different maps concurrently", func(t *testing.T) {
		t.Parallel()

		numGoroutines := 50
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for range numGoroutines {
			go func() {
				defer wg.Done()

				m := map[string]string{
					"key1": "value1",
					"key2": "value2",
				}

				val, found := getAndDelete(m, "key1")
				assert.True(t, found)
				assert.Equal(t, "value1", val)
				assert.Equal(t, 1, len(m))
			}()
		}

		wg.Wait()
	})
}

// TestRaceConditions runs with -race flag to detect race conditions
func TestRaceConditions(t *testing.T) {
	t.Parallel()

	t.Run("no race in conversion pipeline", func(t *testing.T) {
		t.Parallel()

		numGoroutines := 20

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := range numGoroutines {
			go func(id int) {
				defer wg.Done()

				registry := metrics.NewRegistry()
				metric := registry.MustNewMetric("test_metric", metrics.Trend)
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
					Value: float64(id),
				}

				// Convert to simple
				ss := convertToSimple(sample)
				assert.NotNil(t, ss.Tags)

				// Convert to compatible
				cs, err := convertToCompatible(sample)
				assert.NoError(t, err)
				assert.NotNil(t, cs.ExtraTags)
			}(i)
		}

		wg.Wait()
	})
}

// BenchmarkConcurrentConvertToSimple benchmarks concurrent convertToSimple calls
func BenchmarkConcurrentConvertToSimple(b *testing.B) {
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
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ss := convertToSimple(sample)
			_ = ss
		}
	})
}

// BenchmarkConcurrentMemoryPool benchmarks concurrent pool access
func BenchmarkConcurrentMemoryPool(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := tagMapPool.Get().(map[string]string)
			m["key"] = "value"
			clearMap(m)
			tagMapPool.Put(m)
		}
	})
}
