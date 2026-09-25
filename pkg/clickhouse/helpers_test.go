package clickhouse

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	clickhouseModule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// Test constants for ClickHouse container configuration.
const (
	testClickHouseImage  = "clickhouse/clickhouse-server:26.3-alpine"
	testUsername         = "default"
	testPassword         = "password"
	testDatabase         = "default"
	testContainerTimeout = 2 * time.Minute
)

// mockSampleContainer implements metrics.SampleContainer for testing.
type mockSampleContainer struct {
	samples []metrics.Sample
}

func (m *mockSampleContainer) GetSamples() []metrics.Sample {
	return m.samples
}

// newSample builds a metrics.Sample named name with the given type and
// value. A nil tags map produces a sample with a nil TagSet, matching what
// k6 emits for a sample with no tags; a non-nil map is applied to a fresh
// registry's root TagSet.
func newSample(t testing.TB, name string, typ metrics.MetricType, value float64, tags map[string]string) metrics.Sample {
	t.Helper()

	registry := metrics.NewRegistry()
	metric := registry.MustNewMetric(name, typ)

	var tagSet *metrics.TagSet
	if tags != nil {
		tagSet = registry.RootTagSet().WithTagsFromMap(tags)
	}

	return metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: metric,
			Tags:   tagSet,
		},
		Time:  time.Now(),
		Value: value,
	}
}

// newTestLogger creates a logrus logger for testing that discards output.
func newTestLogger(t testing.TB) logrus.FieldLogger {
	t.Helper()
	l := logrus.New()
	l.SetOutput(io.Discard)
	l.SetLevel(logrus.DebugLevel)
	return l
}

// newTestOutput creates a *clickhouseOutput for testing with optional JSON config.
func newTestOutput(t testing.TB, config ...map[string]any) *clickhouseOutput {
	t.Helper()
	var jsonConfig json.RawMessage
	if len(config) > 0 {
		jsonConfig = mustMarshalJSON(config[0])
	}
	out, err := New(output.Params{
		Logger:     newTestLogger(t),
		JSONConfig: jsonConfig,
	})
	require.NoError(t, err)
	return out.(*clickhouseOutput)
}

// mustMarshalJSON marshals v to JSON or panics.
func mustMarshalJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// startClickHouseContainer starts a ClickHouse container for testing and
// returns its native-protocol endpoint (host:port). The container is
// terminated via t.Cleanup.
func startClickHouseContainer(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), testContainerTimeout)
	t.Cleanup(cancel)

	container, err := clickhouseModule.Run(ctx,
		testClickHouseImage,
		clickhouseModule.WithUsername(testUsername),
		clickhouseModule.WithPassword(testPassword),
		clickhouseModule.WithDatabase(testDatabase),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	})

	endpoint, err := container.ConnectionHost(ctx)
	require.NoError(t, err)

	t.Logf("ClickHouse running at %s", endpoint)
	return endpoint
}
