package tripperware

import (
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

// This is a simplified implementation.
// Future versions of the distributed optimizer are expected to:
// - Support more complex query patterns.
// - Incorporate diverse optimization strategies.
// - Extend support to node types beyond binary operations.

type DistributedOptimizer struct{}

func (d *DistributedOptimizer) Optimize(root logicalplan.Node, opts *query.Options) (logicalplan.Node, annotations.Annotations) {
	warns := annotations.New()

	logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {

		if (*current).Type() == logicalplan.BinaryNode {
			ch := (*current).Children()

			for i, child := range ch {
				remoteNode := d.wrapWithRemoteExecution(*child, opts)
				*ch[i] = remoteNode
			}
		}

		return false
	})
	return root, *warns
}

func (d *DistributedOptimizer) wrapWithRemoteExecution(node logicalplan.Node, opts *query.Options) logicalplan.Node {
	// the current version only creates one remote execution for one node, no extra sharding based on time ranges

	remoteNodes := make([]logicalplan.RemoteExecution, 1)

	remoteNodes[0] = logicalplan.RemoteExecution{
		Query:           node.Clone(),
		QueryRangeStart: opts.Start,
		QueryRangeEnd:   opts.End,
	}

	return logicalplan.Deduplicate{
		Expressions: remoteNodes,
	}
}
