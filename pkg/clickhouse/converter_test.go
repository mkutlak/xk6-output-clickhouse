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
