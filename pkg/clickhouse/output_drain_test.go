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
// the DB is nil) must be counted as dropped rather than silently lost. This pins
// the regression where a single unretried drain dropped buffered data without
// any accounting.
func TestStop_DrainsPendingAndAccountsLoss(t *testing.T) {
	t.Parallel()

	params := output.Params{Logger: newTestLogger(t)}
	out, err := New(params)
	require.NoError(t, err)
	o := out.(*Output)

	// Simulate samples buffered during a prior outage. db is nil, so the drain's
	// doFlush fails with a non-retryable error, exercising the loss accounting.
	o.pending = append(o.pending, makeSampleContainer(t).GetSamples()...)
	o.pending = append(o.pending, makeSampleContainer(t).GetSamples()...)
	require.Len(t, o.pending, 2)

	require.NoError(t, o.Stop())

	assert.Empty(t, o.pending, "pending should be emptied by the shutdown drain")

	m := o.GetErrorMetrics()
	assert.Equal(t, uint64(2), m.DroppedSamples,
		"undrainable pending samples must be counted as dropped on shutdown")
}

// TestStart_AfterStop_ReturnsClosedError verifies an Output cannot be restarted
// after Stop(): Start() must reject a closed Output rather than spinning up a
// second periodic flusher.
func TestStart_AfterStop_ReturnsClosedError(t *testing.T) {
	t.Parallel()

	params := output.Params{Logger: newTestLogger(t)}
	out, err := New(params)
	require.NoError(t, err)
	o := out.(*Output)

	require.NoError(t, o.Stop())

	err = o.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "output already closed")
}
