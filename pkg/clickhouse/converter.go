package clickhouse

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.k6.io/k6/metrics"
)

// SimpleSample represents a sample for the simple schema
type SimpleSample struct {
	Timestamp   time.Time
	MetricName  string
	MetricValue float64
	Tags        map[string]string
}

// CompatibleSample represents a sample for the compatible schema
type CompatibleSample struct {
	Timestamp        time.Time
	BuildID          uint32
	Release          string
	Version          string
	Branch           string
	MetricName       string
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

// ConvertToSimple converts a k6 sample to the simple schema format
func ConvertToSimple(ctx context.Context, sample metrics.Sample) SimpleSample {
	ss := SimpleSample{
		Timestamp:   sample.Time,
		MetricName:  sample.Metric.Name,
		MetricValue: sample.Value,
		Tags:        make(map[string]string),
	}

	if sample.Tags != nil {
		for k, v := range sample.Tags.Map() {
			ss.Tags[k] = v
		}
	}

	return ss
}

// ConvertToCompatible converts a k6 sample to the compatible schema format
func ConvertToCompatible(ctx context.Context, sample metrics.Sample) (CompatibleSample, error) {
	cs := CompatibleSample{
		Timestamp:        sample.Time,
		MetricName:       sample.Metric.Name,
		Value:            sample.Value,
		MetricType:       mapMetricType(sample.Metric.Type),
		ExpectedResponse: true, // default
		ExtraTags:        make(map[string]string),
	}

	// Extract and map tags to columns
	if sample.Tags != nil {
		tagMap := sample.Tags.Map()

		// TestID (with aliases)
		if testID, ok := getAndDelete(tagMap, "testid"); ok {
			cs.TestID = testID
		} else if testID, ok := getAndDelete(tagMap, "test_run_id"); ok {
			cs.TestID = testID
		} else {
			cs.TestID = "default"
		}

		// BuildID (with type conversion)
		if buildID, ok := getAndDelete(tagMap, "build_id"); ok {
			if id, err := strconv.ParseUint(buildID, 10, 32); err == nil {
				cs.BuildID = uint32(id)
			} else {
				return cs, fmt.Errorf("failed to parse build_id: %w", err)
			}
		}
		// If not set from tags, generate from timestamp
		if cs.BuildID == 0 {
			cs.BuildID = uint32(time.Now().Unix())
		}

		// String fields
		cs.Release = getAndDeleteWithDefault(tagMap, "release", "")
		cs.Version = getAndDeleteWithDefault(tagMap, "version", "")
		cs.Branch = getAndDeleteWithDefault(tagMap, "branch", "master")
		cs.UIFeature = getAndDeleteWithDefault(tagMap, "ui_feature", "")
		cs.Scenario = getAndDeleteWithDefault(tagMap, "scenario", "")
		cs.Name = getAndDeleteWithDefault(tagMap, "name", "")
		cs.Method = getAndDeleteWithDefault(tagMap, "method", "")
		cs.ErrorCode = getAndDeleteWithDefault(tagMap, "error_code", "")
		cs.Rating = getAndDeleteWithDefault(tagMap, "rating", "")
		cs.ResourceType = getAndDeleteWithDefault(tagMap, "resource_type", "")
		cs.CheckName = getAndDeleteWithDefault(tagMap, "check_name", "")

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

		// ExpectedResponse (with type conversion)
		if expResp, ok := getAndDelete(tagMap, "expected_response"); ok {
			cs.ExpectedResponse = expResp == "true"
		}

		// Remaining tags go to extra_tags
		cs.ExtraTags = tagMap
	} else {
		// No tags, use defaults
		cs.TestID = "default"
		cs.BuildID = uint32(time.Now().Unix())
		cs.Branch = "master"
	}

	return cs, nil
}

// mapMetricType maps k6 metric type to ClickHouse enum value
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

// getAndDelete gets a value from the map and deletes the key
func getAndDelete(m map[string]string, key string) (string, bool) {
	if val, ok := m[key]; ok {
		delete(m, key)
		return val, true
	}
	return "", false
}

// getAndDeleteWithDefault gets a value from the map, deletes the key, and returns a default if not found
func getAndDeleteWithDefault(m map[string]string, key string, defaultValue string) string {
	if val, ok := m[key]; ok {
		delete(m, key)
		return val
	}
	return defaultValue
}
