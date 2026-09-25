package clickhouse

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

func TestConcurrentAddMetricSamples(t *testing.T) {
	t.Parallel()

	clickhouseOut := newTestOutput(t, map[string]any{
		"addr":     "localhost:9000",
		"database": "k6",
		"table":    "samples",
	})

	numGoroutines := 10
	samplesPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

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

	wg.Wait()

	// Exercise GetBufferedSamples under concurrent load; correctness is validated by the race detector.
	buffered := clickhouseOut.GetBufferedSamples()
	_ = buffered
}

func TestConcurrentSimpleSchemaRow(t *testing.T) {
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

			for range 100 {
				row, err := simpleSchema{}.Row(sample)
				if err != nil {
					errors <- err
					return
				}
				if row[1] != "http_reqs" {
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
}

func TestConcurrentCompatSchemaRow(t *testing.T) {
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
				row, err := compatSchema{defaultBuildID: 12345}.Row(sample)
				if err != nil {
					errors <- err
					return
				}
				if row[1] != "http_reqs" {
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
}

// TestErrorMetrics_Concurrency consolidates concurrent error metrics tests
func TestErrorMetrics_Concurrency(t *testing.T) {
	t.Parallel()

	t.Run("concurrent reads are safe", func(t *testing.T) {
		t.Parallel()

		clickhouseOut := newTestOutput(t, map[string]any{
			"addr": "localhost:9000",
		})

		numGoroutines := 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for range numGoroutines {
			go func() {
				defer wg.Done()
				for range 100 {
					errStats := clickhouseOut.GetErrorMetrics()
					_ = errStats.ConvertErrors
					_ = errStats.InsertErrors
					_ = errStats.SamplesProcessed
				}
			}()
		}

		wg.Wait()
	})

	t.Run("concurrent increments are atomic", func(t *testing.T) {
		t.Parallel()

		clickhouseOut := newTestOutput(t, map[string]any{
			"addr": "localhost:9000",
		})

		numGoroutines := 50
		incrementsPerGoroutine := 100
		expectedTotal := uint64(numGoroutines * incrementsPerGoroutine)

		var wg sync.WaitGroup
		wg.Add(numGoroutines * 3)

		for range numGoroutines {
			go func() {
				defer wg.Done()
				for range incrementsPerGoroutine {
					clickhouseOut.convertErrors.Add(1)
				}
			}()
		}

		for range numGoroutines {
			go func() {
				defer wg.Done()
				for range incrementsPerGoroutine {
					clickhouseOut.insertErrors.Add(1)
				}
			}()
		}

		for range numGoroutines {
			go func() {
				defer wg.Done()
				for range incrementsPerGoroutine {
					clickhouseOut.samplesProcessed.Add(1)
				}
			}()
		}

		wg.Wait()

		errStats := clickhouseOut.GetErrorMetrics()
		assert.Equal(t, expectedTotal, errStats.ConvertErrors)
		assert.Equal(t, expectedTotal, errStats.InsertErrors)
		assert.Equal(t, expectedTotal, errStats.SamplesProcessed)
	})

	t.Run("concurrent reads and writes", func(t *testing.T) {
		t.Parallel()

		clickhouseOut := newTestOutput(t, map[string]any{
			"addr": "localhost:9000",
		})

		numWriters := 20
		numReaders := 30
		iterations := 100

		var wg sync.WaitGroup
		wg.Add(numWriters + numReaders)

		for range numWriters {
			go func() {
				defer wg.Done()
				for range iterations {
					clickhouseOut.convertErrors.Add(1)
					clickhouseOut.insertErrors.Add(1)
					clickhouseOut.samplesProcessed.Add(1)
				}
			}()
		}

		for range numReaders {
			go func() {
				defer wg.Done()
				for range iterations {
					errStats := clickhouseOut.GetErrorMetrics()
					assert.GreaterOrEqual(t, errStats.ConvertErrors, uint64(0))
					assert.GreaterOrEqual(t, errStats.InsertErrors, uint64(0))
					assert.GreaterOrEqual(t, errStats.SamplesProcessed, uint64(0))
				}
			}()
		}

		wg.Wait()

		expectedTotal := uint64(numWriters * iterations)
		errStats := clickhouseOut.GetErrorMetrics()
		assert.Equal(t, expectedTotal, errStats.ConvertErrors)
		assert.Equal(t, expectedTotal, errStats.InsertErrors)
		assert.Equal(t, expectedTotal, errStats.SamplesProcessed)
	})
}

// Benchmarks

func BenchmarkConcurrentSimpleSchemaRow(b *testing.B) {
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
			if _, err := (simpleSchema{}).Row(sample); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkGetErrorMetrics(b *testing.B) {
	params := output.Params{
		Logger: newTestLogger(b),
		JSONConfig: mustMarshalJSON(map[string]any{
			"addr": "localhost:9000",
		}),
	}

	out, err := New(params)
	if err != nil {
		b.Fatal(err)
	}

	clickhouseOut := out.(*Output)
	clickhouseOut.convertErrors.Store(1000)
	clickhouseOut.insertErrors.Store(500)
	clickhouseOut.samplesProcessed.Store(100000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			errStats := clickhouseOut.GetErrorMetrics()
			_ = errStats
		}
	})
}

func BenchmarkErrorCounterAdd(b *testing.B) {
	params := output.Params{
		Logger: newTestLogger(b),
		JSONConfig: mustMarshalJSON(map[string]any{
			"addr": "localhost:9000",
		}),
	}

	out, err := New(params)
	if err != nil {
		b.Fatal(err)
	}

	clickhouseOut := out.(*Output)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			clickhouseOut.convertErrors.Add(1)
			clickhouseOut.insertErrors.Add(1)
			clickhouseOut.samplesProcessed.Add(1)
		}
	})
}
