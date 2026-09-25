package clickhouse

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.k6.io/k6/v2/metrics"
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
