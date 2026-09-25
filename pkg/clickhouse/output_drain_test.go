package clickhouse

import (
	"database/sql"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// makeSamples builds one sample per value.
func makeSamples(t *testing.T, values ...float64) metrics.Samples {
	t.Helper()
	registry := metrics.NewRegistry()
	metric := registry.MustNewMetric("test_metric", metrics.Counter)
	samples := make(metrics.Samples, len(values))
	for i, v := range values {
		samples[i] = metrics.Sample{
			TimeSeries: metrics.TimeSeries{Metric: metric, Tags: registry.RootTagSet()},
			Time:       time.Now(),
			Value:      v,
		}
	}
	return samples
}

// negativeFailsSchema fails to convert samples with a negative value.
type negativeFailsSchema struct{ simpleSchema }

func (negativeFailsSchema) Row(s metrics.Sample) ([]any, error) {
	if s.Value < 0 {
		return nil, errors.New("negative value")
	}
	return simpleSchema{}.Row(s)
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
	o.schema = simpleSchema{}

	// Simulate samples buffered during a prior outage. db is nil, so the drain
	// has nowhere to write them, exercising the loss accounting.
	o.pending = append(o.pending, makeSamples(t, 1)...)
	o.pending = append(o.pending, makeSamples(t, 1)...)
	require.Len(t, o.pending, 2)

	start := time.Now()
	require.NoError(t, o.Stop())
	assert.Less(t, time.Since(start), time.Second, "errNotStarted must not be retried")

	assert.Empty(t, o.pending, "pending should be emptied by the shutdown drain")
	assert.Equal(t, uint64(2), o.stats.dropped,
		"undrainable pending samples must be counted as dropped on shutdown")
	assert.Zero(t, o.stats.retries)

	// A second Stop is a no-op: it must not drain or count anything again.
	o.pending = append(o.pending, makeSamples(t, 1)...)
	require.NoError(t, o.Stop())
	assert.Len(t, o.pending, 1, "second Stop must not drain")
	assert.Equal(t, uint64(2), o.stats.dropped)
}

func TestFlush_FailureBuffersSamples(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t)
	o.schema = simpleSchema{}

	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 1, 2), makeSamples(t, 3)})
	o.flush()

	assert.Len(t, o.pending, 3)
	assert.Equal(t, uint64(1), o.stats.flushFailures)
	assert.Zero(t, o.stats.dropped)
	assert.Zero(t, o.stats.written)
	assert.Zero(t, o.stats.retries, "errNotStarted must not be retried")

	// The next flush retries pending samples first, then the new ones.
	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 4)})
	o.flush()

	require.Len(t, o.pending, 4)
	for i, s := range o.pending {
		assert.Equal(t, float64(i+1), s.Value)
	}
	assert.Equal(t, uint64(2), o.stats.flushFailures)
}

func TestFlush_BufferOverflowDropsSamples(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t, map[string]any{"bufferMaxSamples": 2, "bufferDropPolicy": dropOldest})
	o.schema = simpleSchema{}

	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 1, 2, 3)})
	o.flush()

	require.Len(t, o.pending, 2)
	assert.Equal(t, 2.0, o.pending[0].Value)
	assert.Equal(t, uint64(1), o.stats.dropped)
}

func TestFlush_BufferingDisabledDropsSamples(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t, map[string]any{"bufferEnabled": false})
	o.schema = simpleSchema{}

	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 1, 2, 3)})
	o.flush()

	assert.Empty(t, o.pending)
	assert.Equal(t, uint64(3), o.stats.dropped)
	assert.Equal(t, uint64(1), o.stats.flushFailures)
}

// TestWrite_ConvertsOnce guards that conversion happens once per write, not
// once per retry attempt, and that samples failing conversion are not
// re-buffered.
func TestWrite_ConvertsOnce(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t, map[string]any{
		"retryAttempts": 3,
		"retryDelay":    "1ms",
		"retryMaxDelay": "1ms",
	})
	o.schema = negativeFailsSchema{}
	o.db = refusingDB(t)

	o.AddMetricSamples([]metrics.SampleContainer{makeSamples(t, 1, -1, 2, -2, 3)})
	o.flush()

	assert.Equal(t, uint64(2), o.stats.convertErrors, "convert errors must be counted once, not per attempt")
	assert.Equal(t, uint64(3), o.stats.retries, "retries must not count the final attempt")
	assert.Equal(t, uint64(1), o.stats.flushFailures)
	assert.Zero(t, o.stats.written)

	require.Len(t, o.pending, 3, "only converted samples are re-buffered")
	for i, s := range o.pending {
		assert.Equal(t, float64(i+1), s.Value)
	}
}

func TestWrite_AllConvertErrors(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t)
	o.schema = negativeFailsSchema{}

	kept, err := o.write(t.Context(), makeSamples(t, -1, -2))
	require.NoError(t, err, "nothing to insert is not an insert failure")
	assert.Empty(t, kept)
	assert.Equal(t, uint64(2), o.stats.convertErrors)
}

func TestInsertRows_NotStarted(t *testing.T) {
	t.Parallel()

	o := newTestOutput(t)
	err := o.insertRows(t.Context(), [][]any{{1}})
	require.ErrorIs(t, err, errNotStarted)
	assert.False(t, isRetryableError(err))
}

// refusingDB returns a database handle whose connections are refused, which
// isRetryableError treats as transient.
func refusingDB(t *testing.T) *sql.DB {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:        []string{addr},
		DialTimeout: time.Second,
	})
	t.Cleanup(func() { _ = db.Close() })
	return db
}
