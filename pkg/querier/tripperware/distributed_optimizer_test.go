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

func TestDistributedOptimizerAdditionals(t *testing.T) {
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
			result          string
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
				result          string
			}{
				childrenCount:   2,    // binary node should have 2 children
				deduplicateType: true, // each RemoteExecution should be wrapped in Deduplicate
				result:          "dedup(remote(sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(sum(rate(node_memory_Active_bytes[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
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
				result          string
			}{
				childrenCount:   2,
				deduplicateType: true,
				result:          "dedup(remote(sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(sum(rate(node_memory_Active_bytes[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
		{
			name:  "multiple binary operations with aggregations",
			query: "sum(rate(http_requests_total{job=\"api\"}[5m])) + sum(rate(http_requests_total{job=\"web\"}[5m])) - sum(rate(http_requests_total{job=\"cache\"}[5m]))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				deduplicateType bool
				result          string
			}{
				childrenCount:   2,
				deduplicateType: true,
				result:          "dedup(remote(dedup(remote(sum(rate(http_requests_total{job=\"api\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(sum(rate(http_requests_total{job=\"web\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) - dedup(remote(sum(rate(http_requests_total{job=\"cache\"}[5m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
		{
			name:  "aggregation with label replacement",
			query: "sum(rate(container_cpu_usage_seconds_total[1m])) by (pod) + sum(rate(container_memory_usage_bytes[1m])) by (pod)",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				deduplicateType bool
				result          string
			}{
				childrenCount:   2,
				deduplicateType: true,
				result:          "dedup(remote(sum by (pod) (rate(container_cpu_usage_seconds_total[1m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(sum by (pod) (rate(container_memory_usage_bytes[1m]))) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
		{
			name:  "subquery with aggregation",
			query: "sum(rate(container_network_transmit_bytes_total[5m:1m]))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				deduplicateType bool
				result          string
			}{
				childrenCount:   1,
				deduplicateType: true,
				result:          "sum(rate(container_network_transmit_bytes_total[5m:1m]))",
			},
		},
		{
			name:  "avg over vector with offset",
			query: "avg(rate(node_disk_reads_completed_total[5m] offset 1h))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				deduplicateType bool
				result          string
			}{
				childrenCount:   1,
				deduplicateType: true,
				result:          "avg(rate(node_disk_reads_completed_total[5m] offset 1h))",
			},
		},
		{
			name:  "function applied on binary operation",
			query: "rate(http_requests_total[5m]) + rate(http_errors_total[5m]) > bool 0",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				deduplicateType bool
				result          string
			}{
				childrenCount:   2,
				deduplicateType: true,
				result:          "dedup(remote(dedup(remote(rate(http_requests_total[5m])) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) + dedup(remote(rate(http_errors_total[5m])) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC]) > bool dedup(remote(0) [1970-01-01 00:01:40 +0000 UTC, 1970-01-01 00:01:40 +0000 UTC])",
			},
		},
		{
			name:  "aggregation without binary, single child",
			query: "sum(rate(process_cpu_seconds_total[5m]))",
			start: 100000,
			end:   100000,
			step:  time.Minute,
			expected: struct {
				childrenCount   int
				remoteExecCount int
				deduplicateType bool
				result          string
			}{
				childrenCount:   1,
				deduplicateType: true,
				result:          "sum(rate(process_cpu_seconds_total[5m]))",
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

			root := req.LogicalPlan.Root()
			require.Equal(t, tc.expected.result, root.String())
		})
	}
}
