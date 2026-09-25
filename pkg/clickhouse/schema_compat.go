package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"go.k6.io/k6/v2/metrics"
)

// CompatibleSchemaImpl is the legacy compatible schema implementation.
// It extracts known k6 tags into dedicated typed columns for query performance.
//
// This serves as an example of a custom schema implementation. Fork this file
// to create your own schema with the columns you need.
var CompatibleSchemaImpl = SchemaImplementation{
	Name:   "compatible",
	Schema: CompatibleSchema{},
	Converter: CompatibleConverter{
		defaultBuildID: safeUnixToUint32(time.Now().Unix()),
	},
}

func init() {
	RegisterSchema(CompatibleSchemaImpl)
}

// CompatibleSchema implements SchemaCreator for the legacy compatible schema.
//
// Schema structure:
//
//	CREATE TABLE {db}.{table} (
//	    timestamp         DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
//	    metric            LowCardinality(String),
//	    metric_type       Enum8('counter'=1, 'gauge'=2, 'rate'=3, 'trend'=4),
//	    value             Float64 CODEC(Gorilla, ZSTD(1)),
//	    testid            LowCardinality(String) DEFAULT '',
//	    release           LowCardinality(String) DEFAULT '',
//	    scenario          LowCardinality(String) DEFAULT '',
//	    build_id          UInt32 DEFAULT 0 CODEC(Delta, ZSTD(1)),
//	    version           LowCardinality(String) DEFAULT '',
//	    branch            LowCardinality(String) DEFAULT 'master',
//	    name              String DEFAULT '' CODEC(ZSTD(1)),
//	    method            LowCardinality(String) DEFAULT '',
//	    status            UInt16 DEFAULT 0,
//	    expected_response Bool DEFAULT true,
//	    error_code        LowCardinality(String) DEFAULT '',
//	    rating            LowCardinality(String) DEFAULT '',
//	    resource_type     LowCardinality(String) DEFAULT '',
//	    ui_feature        LowCardinality(String) DEFAULT '',
//	    check_name        String DEFAULT '' CODEC(ZSTD(1)),
//	    group_name        LowCardinality(String) DEFAULT '',
//	    extra_tags        Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1))
//	) ENGINE = MergeTree()
//	PARTITION BY toYYYYMM(timestamp)
//	ORDER BY (metric, testid, release, timestamp)
//	TTL toDateTime(timestamp) + INTERVAL 365 DAY DELETE
//	SETTINGS index_granularity = 8192
type CompatibleSchema struct{}

// CreateSchema creates the database and table for the compatible schema.
func (s CompatibleSchema) CreateSchema(ctx context.Context, db *sql.DB, database, table string) error {
	// Defense-in-depth: Validate identifiers before using them
	if !isValidIdentifier(database) {
		return fmt.Errorf("invalid database name: %s (must be alphanumeric + underscore, max 63 chars)", database)
	}
	if !isValidIdentifier(table) {
		return fmt.Errorf("invalid table name: %s (must be alphanumeric + underscore, max 63 chars)", table)
	}

	// Create database
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", escapeIdentifier(database)))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create table with optimized schema
	//nolint:gosec // G201: SQL string formatting is safe - identifiers are validated with isValidIdentifier() (alphanumeric only) and escaped with backticks
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			timestamp         DateTime64(%d, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
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
	`, escapeIdentifier(database), escapeIdentifier(table), TimestampPrecision)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// InsertQuery returns the INSERT statement for the compatible schema.
func (s CompatibleSchema) InsertQuery(database, table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s.%s (
			timestamp, metric, metric_type, value,
			testid, release, scenario, build_id, version, branch,
			name, method, status, expected_response, error_code,
			rating, resource_type, ui_feature, check_name, group_name,
			extra_tags
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, escapeIdentifier(database), escapeIdentifier(table))
}

// compatibleSample represents a sample for the compatible schema.
type compatibleSample struct {
	Timestamp        time.Time
	BuildID          uint32
	Release          string
	Version          string
	Branch           string
	Metric           string
	MetricType       int8
	Value            float64
	TestID           string
	UIFeature        string
	Scenario         string
	Name             string
	Method           string
	Status           uint16
	ExpectedResponse bool
	ErrorCode        string
	Rating           string
	ResourceType     string
	CheckName        string
	GroupName        string
	ExtraTags        map[string]string
}

// convertToCompatible converts a k6 sample to the compatible schema format.
func convertToCompatible(sample metrics.Sample, defaultBuildID uint32) (compatibleSample, error) {
	cs := compatibleSample{
		Timestamp:        sample.Time,
		Metric:           sample.Metric.Name,
		Value:            sample.Value,
		MetricType:       mapMetricType(sample.Metric.Type),
		ExpectedResponse: true, // default
		ExtraTags:        map[string]string{},
	}

	// Extract and map tags to columns
	if sample.Tags != nil {
		// sample.Tags.Map() returns a fresh map on every call, so extraction can
		// delete known keys in place — the leftovers become extra_tags with no
		// extra copy, and k6's internal tag state is never touched.
		cs.ExtraTags = sample.Tags.Map()
		tagMap := cs.ExtraTags

		// TestID (with aliases)
		if testID, ok := getAndDelete(tagMap, "testid"); ok {
			cs.TestID = testID
		} else if testID, ok := getAndDelete(tagMap, "test_run_id"); ok {
			cs.TestID = testID
		} else {
			cs.TestID = "default"
		}

		// BuildID (with type conversion)
		if buildID, ok := getAndDelete(tagMap, "buildId"); ok {
			if id, err := strconv.ParseUint(buildID, 10, 32); err == nil {
				cs.BuildID = uint32(id)
			} else {
				return cs, fmt.Errorf("failed to parse buildId: %w", err)
			}
		}
		// If not set from tags, use the default generated at startup
		if cs.BuildID == 0 {
			cs.BuildID = defaultBuildID
		}

		// String fields
		cs.Release = getAndDeleteWithDefault(tagMap, "release", "")
		cs.Version = getAndDeleteWithDefault(tagMap, "version", "")
		cs.Branch = getAndDeleteWithDefault(tagMap, "branch", "master")
		// UIFeature (with camelCase alias)
		if uiFeature, ok := getAndDelete(tagMap, "ui_feature"); ok {
			cs.UIFeature = uiFeature
		} else {
			cs.UIFeature = getAndDeleteWithDefault(tagMap, "uiFeature", "")
		}
		cs.Scenario = getAndDeleteWithDefault(tagMap, "scenario", "")
		cs.Name = getAndDeleteWithDefault(tagMap, "name", "")
		cs.Method = getAndDeleteWithDefault(tagMap, "method", "")
		cs.ErrorCode = getAndDeleteWithDefault(tagMap, "error_code", "")
		cs.Rating = getAndDeleteWithDefault(tagMap, "rating", "")
		cs.ResourceType = getAndDeleteWithDefault(tagMap, "resource_type", "")
		// CheckName (with alias: k6 native tag is "check", "check_name" is a custom alias)
		if checkName, ok := getAndDelete(tagMap, "check"); ok {
			cs.CheckName = checkName
		} else {
			cs.CheckName = getAndDeleteWithDefault(tagMap, "check_name", "")
		}

		// GroupName (with alias)
		if groupName, ok := getAndDelete(tagMap, "group_name"); ok {
			cs.GroupName = groupName
		} else {
			cs.GroupName = getAndDeleteWithDefault(tagMap, "group", "")
		}

		// Status (with type conversion)
		if statusStr, ok := getAndDelete(tagMap, "status"); ok {
			if statusInt, err := strconv.ParseUint(statusStr, 10, 16); err == nil {
				cs.Status = uint16(statusInt)
			} else {
				return cs, fmt.Errorf("failed to parse status: %w", err)
			}
		}

		// ExpectedResponse: k6 only ever emits "true"/"false" for this tag, so a
		// lenient equality check is sufficient. Any other value is treated as false
		// (rather than failing the whole sample, as a strict parse would).
		if expResp, ok := getAndDelete(tagMap, "expected_response"); ok {
			cs.ExpectedResponse = expResp == "true"
		}

		// Remaining (unrecognized) tags already live in cs.ExtraTags — no extra copy.
	} else {
		// No tags, use defaults
		cs.TestID = "default"
		cs.BuildID = defaultBuildID
		cs.Branch = "master"
	}

	return cs, nil
}

// CompatibleConverter implements SampleConverter for the compatible schema.
// It extracts known k6 tags into dedicated columns with type conversion.
type CompatibleConverter struct {
	// defaultBuildID is set once at creation time and used for all samples
	// that don't provide a buildId tag.
	defaultBuildID uint32
}

// Convert transforms a k6 sample into a row for the compatible schema.
func (c CompatibleConverter) Convert(ctx context.Context, sample metrics.Sample) ([]any, error) {
	cs, err := convertToCompatible(sample, c.defaultBuildID)
	if err != nil {
		return nil, err
	}

	// Row order matches the INSERT query column order.
	return []any{
		cs.Timestamp, cs.Metric, cs.MetricType, cs.Value,
		cs.TestID, cs.Release, cs.Scenario, cs.BuildID, cs.Version, cs.Branch,
		cs.Name, cs.Method, cs.Status, cs.ExpectedResponse, cs.ErrorCode,
		cs.Rating, cs.ResourceType, cs.UIFeature, cs.CheckName, cs.GroupName,
		cs.ExtraTags,
	}, nil
}
