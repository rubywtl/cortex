package distributed_execution

//func TestDistributedExec(t *testing.T) {
//
//	// non-fragmented query
//	now := time.Now()
//	plan := createTestLogicalPlan(t, now, now, 0, "sum() + sum()")
//	oneFrag := []plan_fragments.Fragment{
//		{
//			Node:       plan.Root(),
//			FragmentID: 0,
//			ChildIDs:   []uint64{},
//			IsRoot:     true,
//		},
//	}
//
//	// fragmented query
//	plan2 := createTestLogicalPlan(t, now, now, 0, "sum() + sum()")
//	dOptimizer := DistributedOptimizer{}
//	node2, _, err := dOptimizer.Optimize(plan2.Root())
//	require.NoError(t, err)
//
//	fragments, err := FragmentLogicalPlanNode(uint64(2), node2)
//	require.NoError(t, err)
//
//	// get results
//
//	// compare if they are the same
//}
