package clickhouse

import (
	"fmt"
	"strconv"
	"time"

	"go.k6.io/k6/v2/metrics"
)

func init() {
	RegisterSchema("compatible", compatSchema{defaultBuildID: uint32(time.Now().Unix())})
}

// compatSchema is the legacy "compatible" schema. It extracts well-known k6
// tags into dedicated typed columns, which compress and query better than a
// tag map, and keeps the remaining tags in extra_tags. Fork it as a starting
// point for a custom schema with the columns you need.
type compatSchema struct {
	// defaultBuildID is used for samples without a non-zero buildId tag.
	defaultBuildID uint32
}

func (compatSchema) CreateTable(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			timestamp         DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
			metric            LowCardinality(String),
			metric_type       Enum8('counter'=1, 'gauge'=2, 'rate'=3, 'trend'=4),
			value             Float64 CODEC(Gorilla, ZSTD(1)),
			testid            LowCardinality(String) DEFAULT '',
			release           LowCardinality(String) DEFAULT '',
			scenario          LowCardinality(String) DEFAULT '',
			build_id          UInt32 DEFAULT 0 CODEC(Delta, ZSTD(1)),
			version           LowCardinality(String) DEFAULT '',
			branch            LowCardinality(String) DEFAULT 'master',
			name              String DEFAULT '' CODEC(ZSTD(1)),
			method            LowCardinality(String) DEFAULT '',
			status            UInt16 DEFAULT 0,
			expected_response Bool DEFAULT true,
			error_code        LowCardinality(String) DEFAULT '',
			rating            LowCardinality(String) DEFAULT '',
			resource_type     LowCardinality(String) DEFAULT '',
			ui_feature        LowCardinality(String) DEFAULT '',
			check_name        String DEFAULT '' CODEC(ZSTD(1)),
			group_name        LowCardinality(String) DEFAULT '',
			extra_tags        Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1))
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (metric, testid, release, timestamp)
		TTL toDateTime(timestamp) + INTERVAL 365 DAY DELETE
		SETTINGS index_granularity = 8192
	`, table)
}

func (compatSchema) InsertQuery(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (
			timestamp, metric, metric_type, value,
			testid, release, scenario, build_id, version, branch,
			name, method, status, expected_response, error_code,
			rating, resource_type, ui_feature, check_name, group_name,
			extra_tags
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, table)
}

func (c compatSchema) Row(s metrics.Sample) ([]any, error) {
	// Known tags are removed from tags as they are read; the rest become extra_tags.
	tags := tagMap(s)

	buildID := c.defaultBuildID
	if v, ok := pop(tags, "buildId"); ok {
		id, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse buildId: %w", err)
		}
		if id != 0 {
			buildID = uint32(id)
		}
	}

	var status uint16
	if v, ok := pop(tags, "status"); ok {
		n, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("failed to parse status: %w", err)
		}
		status = uint16(n)
	}

	// k6 only emits "true"/"false" here; anything else counts as false rather
	// than failing the whole sample.
	expectedResponse := true
	if v, ok := pop(tags, "expected_response"); ok {
		expectedResponse = v == "true"
	}

	testID := take(tags, "default", "testid", "test_run_id")
	release := take(tags, "", "release")
	scenario := take(tags, "", "scenario")
	version := take(tags, "", "version")
	branch := take(tags, "master", "branch")
	name := take(tags, "", "name")
	method := take(tags, "", "method")
	errorCode := take(tags, "", "error_code")
	rating := take(tags, "", "rating")
	resourceType := take(tags, "", "resource_type")
	uiFeature := take(tags, "", "ui_feature", "uiFeature")
	checkName := take(tags, "", "check", "check_name")
	groupName := take(tags, "", "group_name", "group")

	return []any{
		s.Time, s.Metric.Name, metricTypeEnum(s.Metric.Type), s.Value,
		testID, release, scenario, buildID, version, branch,
		name, method, status, expectedResponse, errorCode,
		rating, resourceType, uiFeature, checkName, groupName,
		tags,
	}, nil
}

// take returns the value of the first key present in tags and deletes only
// that key, so a losing alias stays in tags. It returns def if none is present.
func take(tags map[string]string, def string, keys ...string) string {
	for _, key := range keys {
		if v, ok := pop(tags, key); ok {
			return v
		}
	}
	return def
}

// pop returns the value of key and deletes it from tags.
func pop(tags map[string]string, key string) (string, bool) {
	v, ok := tags[key]
	delete(tags, key)
	return v, ok
}

// metricTypeEnum maps a k6 metric type to its metric_type Enum8 value.
func metricTypeEnum(mt metrics.MetricType) int8 {
	switch mt {
	case metrics.Counter:
		return 1
	case metrics.Gauge:
		return 2
	case metrics.Rate:
		return 3
	default:
		return 4 // trend
	}
}
