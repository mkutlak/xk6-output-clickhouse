package clickhouse

import (
	"go.k6.io/k6/metrics"
)

const (
	// TimestampPrecision is the precision for DateTime64 (3 = milliseconds)
	TimestampPrecision = 3
)

// mapMetricType maps k6 metric type to ClickHouse enum value.
func mapMetricType(mt metrics.MetricType) int8 {
	switch mt {
	case metrics.Counter:
		return 1
	case metrics.Gauge:
		return 2
	case metrics.Rate:
		return 3
	case metrics.Trend:
		return 4
	default:
		return 4 // Default to trend
	}
}

// getAndDelete gets a value from the map and deletes the key.
func getAndDelete(m map[string]string, key string) (string, bool) {
	if val, ok := m[key]; ok {
		delete(m, key)
		return val, true
	}
	return "", false
}

// getAndDeleteWithDefault gets a value from the map, deletes the key, and returns a default if not found.
func getAndDeleteWithDefault(m map[string]string, key, defaultValue string) string {
	if val, ok := m[key]; ok {
		delete(m, key)
		return val
	}
	return defaultValue
}

// safeUnixToUint32 safely converts a Unix timestamp to uint32, clamping to max uint32 if overflow.
func safeUnixToUint32(unix int64) uint32 {
	const maxUint32 = 1<<32 - 1
	if unix < 0 {
		return 0
	}
	if unix > maxUint32 {
		return maxUint32
	}
	return uint32(unix)
}
