package tripperware

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogicalPlanGenWithPromReq(t *testing.T) {
	testCases := []struct {
		name      string
		queryType string // "instant" or "range"
		input     *PrometheusRequest
		err       error
	}{
		{
			name:      "instant - rate vector selector",
			queryType: "instant",
			input: &PrometheusRequest{
				Start:           100000,
				End:             100000,
				Query:           "rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])",
				DistributedExec: true,
			},
		},
		{
			name:      "instant - rate vector selector",
			queryType: "instant",
			input: &PrometheusRequest{
				Start:           100000,
				End:             100000,
				Query:           "rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])",
				DistributedExec: false,
			},
		},
		{
			name:      "instant - memory usage expression",
			queryType: "instant",
			input: &PrometheusRequest{
				Start:           100000,
				End:             100000,
				Query:           "100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))",
				DistributedExec: true,
			},
		},
		{
			name:      "instant - scalar only query",
			queryType: "instant",
			input: &PrometheusRequest{
				Start:           100000,
				End:             100000,
				Query:           "42",
				DistributedExec: false,
			},
		},
	}

	for i, tc := range testCases {
		tc := tc
		t.Run(strconv.Itoa(i)+"_"+tc.name, func(t *testing.T) {
			t.Parallel()

			middleware := DistributedQueryMiddleware(time.Minute, 5*time.Minute)

			handler := middleware.Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, nil
			}))

			// additional validation on the test cases based on query type
			if tc.queryType == "range" {
				require.NotZero(t, tc.input.Step, "range query should have non-zero step")
				require.NotEqual(t, tc.input.Start, tc.input.End, "range query should have different start and end times")
			} else {
				require.Equal(t, tc.input.Start, tc.input.End, "instant query should have equal start and end times")
				require.Zero(t, tc.input.Step, "instant query should have zero step")
			}

			// test: execute middleware to populate the logical plan
			_, err := handler.Do(context.Background(), tc.input)
			require.NoError(t, err)

			if tc.input.DistributedExec {
				require.NotEmpty(t, tc.input.LogicalPlan, "logical plan should be populated")
			} else {
				require.Empty(t, tc.input.LogicalPlan, "logical plan should be empty")
			}

		})
	}
}
