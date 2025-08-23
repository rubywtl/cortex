package distributed_execution

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

// This test makes sure that the fragmenter fragments logical plan into the correct number of sub-plans
// (the number of fragments also depends on the distributed optimizer
// , so if it changes the expected value will also need to be adjusted)

func TestFragmenter(t *testing.T) {

	lp3 := createTestLogicalPlan(t, time.Now(), time.Now(), 0, "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m]))")
	res3, err := FragmentLogicalPlanNode(0, lp3.Root())
	require.NoError(t, err)
	require.Equal(t, 3, len(res3))

	lp := createTestLogicalPlan(t, time.Now(), time.Now(), 0, "sum(rate(node_cpu_seconds_total{mode!=\"idle\"}[5m])) + sum(rate(node_memory_Active_bytes[5m]))")
	res, err := FragmentLogicalPlanNode(0, lp.Root())
	require.NoError(t, err)
	require.Equal(t, 7, len(res))

	lp2 := createTestLogicalPlan(t, time.Now(), time.Now(), 0, "sum(rate(http_requests_total{job=\"api\"}[5m])) + sum(rate(http_requests_total{job=\"web\"}[5m])) - sum(rate(http_requests_total{job=\"cache\"}[5m]))")
	res2, err2 := FragmentLogicalPlanNode(0, lp2.Root())
	require.NoError(t, err2)
	require.Equal(t, 11, len(res2))
}

func createTestLogicalPlan(t *testing.T, startTime time.Time, endTime time.Time, step time.Duration, q string) logicalplan.Plan {
	qOpts := query.Options{
		Start:              startTime,
		End:                startTime,
		Step:               0,
		StepsBatch:         10,
		LookbackDelta:      0,
		EnablePerStepStats: false,
	}

	if step != 0 {
		qOpts.End = endTime
		qOpts.Step = step
	}

	expr, err := parser.NewParser(q, parser.WithFunctions(parser.Functions)).ParseExpr()
	require.NoError(t, err)

	planOpts := logicalplan.PlanOptions{
		DisableDuplicateLabelCheck: false,
	}

	logicalPlan, err := logicalplan.NewFromAST(expr, &qOpts, planOpts)
	require.NoError(t, err)

	optimizedPlan, _ := logicalPlan.Optimize(logicalplan.DefaultOptimizers)

	dOptimizer := DistributedOptimizer{}
	dOptimizedPlanNode, _, _ := dOptimizer.Optimize(optimizedPlan.Root())
	lp := logicalplan.New(dOptimizedPlanNode, &qOpts, planOpts)

	return lp
}
