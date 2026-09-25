package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/ext"
	"go.k6.io/k6/v2/output"
)

// TestRegistration verifies that init() registered the extension as an
// output.Output constructor under the name "xk6-clickhouse", and that the
// registered constructor produces a working output for empty params.
func TestRegistration(t *testing.T) {
	t.Parallel()

	e, ok := ext.Get(ext.OutputExtension)["xk6-clickhouse"]
	require.True(t, ok, `"xk6-clickhouse" must be registered as an output extension`)

	constructor, ok := e.Module.(output.Constructor)
	require.True(t, ok, "registered module must be an output.Constructor")

	out, err := constructor(output.Params{})
	require.NoError(t, err)
	require.NotNil(t, out)

	assert.Contains(t, out.Description(), "clickhouse")
	assert.NoError(t, out.Stop())
}
