package distributed_execution

import (
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/promql-engine/logicalplan"
)

// This is a simplified implementation that only handles binary aggregation cases
// Future versions of the distributed optimizer are expected to:
// - Support more complex query patterns
// - Incorporate diverse optimization strategies
// - Extend support to node types beyond binary operations

type DistributedOptimizer struct{}

func (d *DistributedOptimizer) Optimize(root logicalplan.Node) (logicalplan.Node, annotations.Annotations, error) {
	warns := annotations.New()

	if root == nil {
		return nil, *warns, fmt.Errorf("nil root node")
	}

	//logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {
	//	if aggr, ok := (*current).(*logicalplan.Aggregation); ok {
	//		// count -> sum( count(shard1) + count(shard2))
	//		if aggr.Op == parser.COUNT {
	//			subqueries := newRemoteAggregation(aggr, 2)
	//			*current = &logicalplan.Aggregation{
	//				Op:       parser.SUM,
	//				Expr:     ShardedRemoteExecutions{Expressions: subqueries},
	//				Param:    aggr.Param,
	//				Grouping: aggr.Grouping,
	//				Without:  aggr.Without,
	//			}
	//		}
	//
	//		return true
	//	}
	//
	//	return false
	//})

	logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) (stop bool) {
		if aggr, ok := (*current).(*logicalplan.Aggregation); ok {
			// sum -> sum( count(shard1) + count(shard2))
			if aggr.Op == parser.SUM {
				subqueries := newRemoteAggregation(aggr, 2)
				*current = &logicalplan.Aggregation{
					Op:       parser.SUM,
					Expr:     ShardedRemoteExecutions{Expressions: subqueries},
					Param:    aggr.Param,
					Grouping: aggr.Grouping,
					Without:  aggr.Without,
				}
				// count -> sum(count, count)
			} else if aggr.Op == parser.COUNT {
				subqueries := newRemoteAggregation(aggr, 2)
				*current = &logicalplan.Aggregation{
					Op:       parser.SUM,
					Expr:     ShardedRemoteExecutions{Expressions: subqueries},
					Param:    aggr.Param,
					Grouping: aggr.Grouping,
					Without:  aggr.Without,
				}
			}
			return true
		}
		return false
	})

	logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {

		if (*current).Type() == logicalplan.BinaryNode {
			ch := (*current).Children()

			for _, child := range ch {
				temp := (*child).Clone()
				*child = NewRemoteNode()
				*(*child).Children()[0] = temp
			}
		}

		return false
	})

	return root, *warns, nil
}

func insertShardNum(root logicalplan.Node, shardCount int, shardIdx int) logicalplan.Node {
	logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {
		if (*current).Type() == logicalplan.VectorSelectorNode {
			cur := (*current).(*logicalplan.VectorSelector)

			cur.LabelMatchers = append(cur.LabelMatchers, labels.MustNewMatcher(labels.MatchEqual, "__cortex_ingester_shard__", fmt.Sprintf("%d_%d", shardCount, shardIdx)))
		}
		return false
	})
	return root
}

func newRemoteAggregation(rootAggregation *logicalplan.Aggregation, shardNum int) []logicalplan.Node {
	nodes := []logicalplan.Node{}

	for i := 0; i < shardNum; i++ {
		rc := rootAggregation.Expr.Clone()
		rc = insertShardNum(&Remote{
			Expr: &logicalplan.Aggregation{
				Op:       rootAggregation.Op,
				Expr:     rc,
				Param:    rootAggregation.Param,
				Grouping: rootAggregation.Grouping,
			}}, shardNum, i)
		nodes = append(nodes, rc)
	}

	return nodes
}
