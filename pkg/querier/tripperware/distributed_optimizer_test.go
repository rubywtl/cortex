package tripperware

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Before:
//      binary node: sum(a) + sum(b)
//           /               \
//   aggr: sum(a)          aggr: sum(b)
//        |                       |
//   vector selector        vector selector

// After dummy distributed optimizer:
//      binary node: sum(a) + sum(b)
//           /               \
//    remote exec            remote exec
//        |                       |
//   aggr: sum(a)          aggr: sum(b)
//        |                       |
//   vector selector        vector selector

func TestDistributedOptimizer(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		start    int64
		end      int64
		step     time.Duration
		expected struct {
			childrenCount   int
			remoteExecCount int
			deduplicateType bool
		}
	}{
		{
			name:  "binary operation with aggregations",
			query: "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])) + sum(rate(node_memory_Active_bytes[5m]))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				deduplicateType bool
			}{
				childrenCount:   2,    // binary node should have 2 children
				deduplicateType: true, // each RemoteExecution should be wrapped in Deduplicate
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			req := &PrometheusRequest{
				Start: tc.start,
				End:   tc.end,
				Query: tc.query,
			}

			middleware := DistributedQueryMiddleware(tc.step, 5*time.Minute)
			handler := middleware.Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, nil
			}))
			_, err := handler.Do(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, req.LogicalPlan, "logical plan should be populated")

			startNode := req.LogicalPlan.Root().Children()[0]
			children := (*startNode).Children()
			require.Len(t, children, tc.expected.childrenCount)

			LHS := *children[0]
			RHS := *children[1]

			require.Equal(t, LHS.String(), "dedup(remote(sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])")
			require.Equal(t, RHS.String(), "dedup(remote(sum(rate(node_memory_Active_bytes[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])")
		})
	}
}
