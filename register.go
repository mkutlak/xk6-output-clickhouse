package clickhouse

import (
	"github.com/mkutlak/xk6-output-clickhouse/pkg/clickhouse"
	"go.k6.io/k6/output"
)

func init() {
	output.RegisterExtension("clickhouse", func(params output.Params) (output.Output, error) {
		return clickhouse.New(params)
	})
}
