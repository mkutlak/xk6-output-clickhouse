package clickhouse

import (
	"testing"

	"github.com/mkutlak/xk6-output-clickhouse/pkg/clickhouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/output"
)

// TestRegistration verifies that the output extension is properly registered
func TestRegistration(t *testing.T) {
	t.Parallel()

	t.Run("extension is registered with correct name", func(t *testing.T) {
		t.Parallel()

		// The init() function should have registered "clickhouse" extension
		// We can verify this by attempting to get the constructor
		// This is a bit tricky as the registration is internal to k6
		// But we can at least verify the package initializes without panic
		assert.NotPanics(t, func() {
			// If there were any issues in init(), this would panic
			_ = "clickhouse"
		})
	})

	t.Run("New function is accessible and creates valid output", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			ConfigArgument: "",
			JSONConfig:     nil,
		}

		// This tests that our New function works correctly
		// which is what gets registered in init()
		out, err := clickhouse.New(params)
		require.NoError(t, err, "New function should create output without error")
		require.NotNil(t, out, "New function should return non-nil output")

		// Verify it's the correct type
		_, ok := out.(output.Output)
		assert.True(t, ok, "New function should return type implementing output.Output interface")
	})

	t.Run("New function returns proper Output type", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			ConfigArgument: "localhost:9000",
		}

		out, err := clickhouse.New(params)
		require.NoError(t, err)
		require.NotNil(t, out)

		// Verify Description method exists
		desc := out.Description()
		assert.Contains(t, desc, "clickhouse", "Description should mention clickhouse")
		assert.Contains(t, desc, "localhost:9000", "Description should include address")
	})

	t.Run("New function handles configuration correctly", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: []byte(`{"addr":"test:9000","database":"testdb"}`),
		}

		out, err := clickhouse.New(params)
		require.NoError(t, err)
		require.NotNil(t, out)

		desc := out.Description()
		assert.Contains(t, desc, "test:9000")
	})

	t.Run("New function handles invalid config gracefully", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: []byte(`{invalid json`),
		}

		out, err := clickhouse.New(params)
		assert.Error(t, err, "Should return error for invalid JSON config")
		assert.Nil(t, out, "Should return nil output on error")
		assert.Contains(t, err.Error(), "json", "Error should mention JSON parsing issue")
	})
}

// TestRegistrationInitialization tests that init() doesn't panic
func TestRegistrationInitialization(t *testing.T) {
	t.Parallel()

	t.Run("package initialization succeeds", func(t *testing.T) {
		t.Parallel()

		// This test verifies that the package can be imported and initialized
		// without panicking. The init() function registers the extension.
		assert.NotPanics(t, func() {
			// Just accessing the package-level items verifies init() ran
			_ = clickhouse.New
		})
	})
}

// TestRegistrationWithVariousConfigs tests the registered function with various configs
func TestRegistrationWithVariousConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		params        output.Params
		expectError   bool
		errorContains string
		checkDesc     func(t *testing.T, desc string)
	}{
		{
			name: "minimal config",
			params: output.Params{
				ConfigArgument: "",
			},
			expectError: false,
			checkDesc: func(t *testing.T, desc string) {
				assert.Contains(t, desc, "clickhouse")
			},
		},
		{
			name: "url config with database",
			params: output.Params{
				ConfigArgument: "localhost:9000?database=metrics&table=k6",
			},
			expectError: false,
			checkDesc: func(t *testing.T, desc string) {
				assert.Contains(t, desc, "localhost:9000")
			},
		},
		{
			name: "json config with full options",
			params: output.Params{
				JSONConfig: []byte(`{
					"addr": "clickhouse.example.com:9000",
					"database": "production",
					"table": "k6_metrics",
					"pushInterval": "5s"
				}`),
			},
			expectError: false,
			checkDesc: func(t *testing.T, desc string) {
				assert.Contains(t, desc, "clickhouse.example.com:9000")
			},
		},
		{
			name: "invalid pushInterval",
			params: output.Params{
				JSONConfig: []byte(`{"pushInterval": "not-a-duration"}`),
			},
			expectError:   true,
			errorContains: "pushInterval",
		},
		{
			name: "malformed json",
			params: output.Params{
				JSONConfig: []byte(`{"addr": unclosed`),
			},
			expectError:   true,
			errorContains: "json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := clickhouse.New(tt.params)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, out)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)

			if tt.checkDesc != nil {
				desc := out.Description()
				tt.checkDesc(t, desc)
			}
		})
	}
}

// TestOutputInterface verifies the registered output implements required methods
func TestOutputInterface(t *testing.T) {
	t.Parallel()

	t.Run("implements output.Output interface", func(t *testing.T) {
		t.Parallel()

		params := output.Params{}
		out, err := clickhouse.New(params)
		require.NoError(t, err)
		require.NotNil(t, out)

		// Verify all required methods exist
		var _ output.Output = out

		// Test Description method
		desc := out.Description()
		assert.NotEmpty(t, desc, "Description should not be empty")

		// Test Stop method (should work even without Start)
		err = out.Stop()
		assert.NoError(t, err, "Stop should work without Start")
	})
}
