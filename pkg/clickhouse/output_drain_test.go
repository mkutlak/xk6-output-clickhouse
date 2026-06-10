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

// TestStop_DrainsFailoverBufferAndAccountsLoss guards the shutdown drain path:
// the buffer must be emptied, and samples that cannot be drained (here, because
// the DB is nil) must be counted as dropped rather than silently lost. This pins
// the regression where a single unretried drain dropped buffered data without
// any accounting.
func TestStop_DrainsFailoverBufferAndAccountsLoss(t *testing.T) {
	t.Parallel()

	params := output.Params{Logger: newTestLogger(t)}
	out, err := New(params)
	require.NoError(t, err)
	o := out.(*Output)

	// Simulate samples buffered during a prior outage. db is nil, so the drain's
	// doFlush fails with a non-retryable error, exercising the loss accounting.
	o.failoverBuffer = NewSampleBuffer(100, DropOldest)
	dropped := o.failoverBuffer.Push([]metrics.SampleContainer{
		makeSampleContainer(t),
		makeSampleContainer(t),
	})
	require.Zero(t, dropped, "precondition: nothing dropped on push")
	require.Equal(t, 2, o.failoverBuffer.Len())

	require.NoError(t, o.Stop())

	assert.Equal(t, 0, o.failoverBuffer.Len(), "buffer should be emptied by the shutdown drain")

	m := o.GetErrorMetrics()
	assert.Equal(t, uint64(2), m.DroppedSamples,
		"undrainable buffered containers must be counted as dropped on shutdown")
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
