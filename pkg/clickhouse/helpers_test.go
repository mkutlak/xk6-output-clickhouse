package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	clickhouseModule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

// Test constants for ClickHouse container configuration.
const (
	testClickHouseImage  = "clickhouse/clickhouse-server:25.3-alpine"
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

func newMockContainer(id int) metrics.SampleContainer {
	return &mockSampleContainer{
		samples: []metrics.Sample{
			{Value: float64(id)},
		},
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

// newTestOutput creates an *Output for testing with optional JSON config.
func newTestOutput(t testing.TB, config ...map[string]any) *Output {
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
	return out.(*Output)
}

// mustMarshalJSON marshals v to JSON or panics.
func mustMarshalJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// StartClickHouseContainer starts a ClickHouse container for testing.
// Returns the endpoint address (host:port) and a cleanup function.
func StartClickHouseContainer(t *testing.T) (endpoint string, cleanup func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), testContainerTimeout)

	clickhouseContainer, err := clickhouseModule.Run(ctx,
		testClickHouseImage,
		clickhouseModule.WithUsername(testUsername),
		clickhouseModule.WithPassword(testPassword),
		clickhouseModule.WithDatabase(testDatabase),
	)
	require.NoError(t, err)

	cleanup = func() {
		cancel()
		if err := clickhouseContainer.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}

	endpoint, err = clickhouseContainer.PortEndpoint(ctx, "9000/tcp", "")
	if err != nil {
		cleanup()
		require.NoError(t, err)
	}

	t.Logf("ClickHouse running at %s", endpoint)
	return endpoint, cleanup
}

// CreateDatabase creates a database in the ClickHouse instance.
func CreateDatabase(t *testing.T, endpoint, dbName string) {
	t.Helper()

	conn, err := clickhouse_go.Open(&clickhouse_go.Options{
		Addr: []string{endpoint},
		Auth: clickhouse_go.Auth{
			Database: testDatabase,
			Username: testUsername,
			Password: testPassword,
		},
	})
	require.NoError(t, err)

	err = conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}
