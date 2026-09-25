package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// makeSampleContainer builds a SampleContainer with a single sample for tests.
func makeSampleContainer(t *testing.T) metrics.SampleContainer {
	t.Helper()
	registry := metrics.NewRegistry()
	metric := registry.MustNewMetric("test_metric", metrics.Counter)
	return metrics.Samples{
		metrics.Sample{
			TimeSeries: metrics.TimeSeries{Metric: metric},
			Time:       time.Now(),
			Value:      1.0,
		},
	}
}

// TestStop_DrainsPendingAndAccountsLoss guards the shutdown drain path:
// pending must be emptied, and samples that cannot be drained (here, because
// Start never ran) must be counted as dropped rather than silently lost.
func TestStop_DrainsPendingAndAccountsLoss(t *testing.T) {
	t.Parallel()

	params := output.Params{Logger: newTestLogger(t)}
	out, err := New(params)
	require.NoError(t, err)
	o := out.(*Output)

	// Simulate samples buffered during a prior outage. db is nil, so the drain
	// has nowhere to write them, exercising the loss accounting.
	o.pending = append(o.pending, makeSampleContainer(t).GetSamples()...)
	o.pending = append(o.pending, makeSampleContainer(t).GetSamples()...)
	require.Len(t, o.pending, 2)

	require.NoError(t, o.Stop())

	assert.Empty(t, o.pending, "pending should be emptied by the shutdown drain")

	m := o.GetErrorMetrics()
	assert.Equal(t, uint64(2), m.DroppedSamples,
		"undrainable pending samples must be counted as dropped on shutdown")

	// A second Stop is a no-op: it must not drain or count anything again.
	o.pending = append(o.pending, makeSampleContainer(t).GetSamples()...)
	require.NoError(t, o.Stop())
	assert.Len(t, o.pending, 1, "second Stop must not drain")
	assert.Equal(t, uint64(2), o.GetErrorMetrics().DroppedSamples)
}
