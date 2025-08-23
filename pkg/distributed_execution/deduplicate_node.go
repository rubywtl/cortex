package distributed_execution

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/execution"
	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

type ShardedRemoteExecutions struct {
	Expressions []logicalplan.Node `json:"-"`
}

func (r *ShardedRemoteExecutions) MakeExecutionOperator(
	ctx context.Context,
	vectors *model.VectorPool,
	opts *query.Options,
	hints storage.SelectHints,
) (model.VectorOperator, error) {
	operators := make([]model.VectorOperator, len(r.Expressions))
	var err error
	for i := 0; i < len(operators); i++ {
		operators[i], err = execution.New(ctx, r.Expressions[i], nil, opts)
		if err != nil {
			return nil, err
		}
	}
	coalesce := exchange.NewCoalesce(vectors, opts, 0, operators...)
	return exchange.NewConcurrent(coalesce, 2, opts), nil
}

func copyHints(hints storage.SelectHints) storage.SelectHints {
	return storage.SelectHints{
		Start:           hints.Start,
		End:             hints.End,
		Limit:           hints.Limit,
		Step:            hints.Step,
		Func:            hints.Func,
		Grouping:        hints.Grouping,
		By:              hints.By,
		Range:           hints.Range,
		ShardCount:      hints.ShardCount,
		ShardIndex:      hints.ShardIndex,
		DisableTrimming: hints.DisableTrimming,
	}
}

func (r ShardedRemoteExecutions) Clone() logicalplan.Node {
	clone := r
	clone.Expressions = make([]logicalplan.Node, len(r.Expressions))
	for i, e := range r.Expressions {
		clone.Expressions[i] = e.Clone().(*Remote)
	}
	return clone
}

func (r ShardedRemoteExecutions) Children() []*logicalplan.Node {
	return []*logicalplan.Node{&r.Expressions[0], &r.Expressions[1]}
}

func (r ShardedRemoteExecutions) String() string {
	return fmt.Sprintf("shard(%s)", "str")
}

func (r ShardedRemoteExecutions) ReturnType() parser.ValueType { return RemoteNode }

func (r ShardedRemoteExecutions) Type() logicalplan.NodeType { return ShardedRemoteExecutionNode }
