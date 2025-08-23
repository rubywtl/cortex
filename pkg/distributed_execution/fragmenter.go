package distributed_execution

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/thanos-io/promql-engine/logicalplan"
)

type Fragment struct {
	Node       logicalplan.Node
	FragmentID uint64
	ChildIDs   []uint64
	IsRoot     bool
}

func getNewID() uint64 {
	id := uuid.New()
	return binary.BigEndian.Uint64(id[:8])
}

func (s *Fragment) IsEmpty() bool {
	if s.Node != nil {
		return false
	}
	if s.FragmentID != 0 {
		return false
	}
	if s.IsRoot {
		return false
	}
	if len(s.ChildIDs) != 0 {
		return false
	}
	return true
}

// FragmentLogicalPlanNode fragment the logical plan by the remote node
// and inserts the child fragment information into it
func FragmentLogicalPlanNode(queryID uint64, node logicalplan.Node) ([]Fragment, error) {
	newFragment := Fragment{}
	fragments := []Fragment{}

	childIDs := []uint64{}
	nextChildIDs := []uint64{}
	var prevID uint64

	logicalplan.TraverseBottomUp(nil, &node, func(parent, current *logicalplan.Node) bool {

		curlen := len(childIDs)

		if parent == nil { // root fragment
			if len(nextChildIDs) < 2 {
				newFragment = Fragment{
					Node:       node,
					FragmentID: getNewID(),
					ChildIDs:   []uint64{},
					IsRoot:     true,
				}
			} else {
				newFragment = Fragment{
					Node:       node,
					FragmentID: getNewID(),
					ChildIDs:   []uint64{nextChildIDs[len(nextChildIDs)-2], nextChildIDs[len(nextChildIDs)-1]},
					IsRoot:     true,
				}
			}

			fragments = append(fragments, newFragment)

		} else if RemoteNode == (*current).Type() {
			nextChildIDs = append(nextChildIDs, prevID)

		} else if RemoteNode == (*parent).Type() {
			if curlen <= 2 {
				newFragment = Fragment{
					Node:       *current,
					FragmentID: getNewID(),
					ChildIDs:   []uint64{},
					IsRoot:     false,
				}
				childIDs = append(childIDs, newFragment.FragmentID)
			} else {
				newFragment = Fragment{
					Node:       node,
					FragmentID: getNewID(),
					ChildIDs:   []uint64{childIDs[curlen-2], childIDs[curlen-1]},
					IsRoot:     false,
				}
				childIDs = []uint64{}
			}
			prevID = newFragment.FragmentID
			fragments = append(fragments, newFragment)

			// append remote node information that will be used in the execution stage
			key := MakeFragmentKey(queryID, newFragment.FragmentID)
			(*parent).(*Remote).FragmentKey = key
		}
		return false
	})

	if fragments != nil {
		return fragments, nil
	} else {
		// for non-query API calls
		// --> treat as root fragment and immediately return the result
		return []Fragment{{
			Node:       node,
			FragmentID: uint64(0),
			ChildIDs:   []uint64{},
			IsRoot:     true,
		}}, nil
	}
}

//
//func TraverseDown(parent *Node, current *Node, layer int,
//	transform func(parent *Node, current *Node, layer int) (bool, bool)) (bool, bool) {
//	var stop bool
//	layer = layer + 1
//
//	for _, c := range (*current).Children() {
//		newStop, _ := TraverseDown(current, c, layer, transform)
//		stop = newStop || stop
//	}
//	if stop {
//		return false, false
//	}
//	return transform(parent, current, layer)
//}
//
//func FragmentLogicalPlanNode2(queryID uint64, node logicalplan.Node) ([]Fragment, error) {
//	newFragment := Fragment{}
//	fragments := []Fragment{}
//
//	nextChildIDs := make(map[int][]uint64, 0)
//
//	layer := 0
//
//	TraverseDown(nil, &node, layer, func(parent, current *logicalplan.Node, layer int) (stop bool, remote bool) {
//
//		if parent == nil { // root fragment
//			newFragment = Fragment{
//				Node:       node,
//				FragmentID: getNewID(),
//				ChildIDs:   nextChildIDs[layer],
//				IsRoot:     true,
//			}
//			fragments = append(fragments, newFragment)
//			return false, true
//		} else if RemoteNode == (*parent).Type() {
//			newFragment = Fragment{
//				Node:       node,
//				FragmentID: getNewID(),
//				ChildIDs:   nextChildIDs[layer],
//				IsRoot:     false,
//			}
//			return false, true
//		}
//
//		nextChildIDs[layer-1] = append(nextChildIDs[layer-1], newFragment.FragmentID)
//
//		fragments = append(fragments, newFragment)
//
//		// append remote node information that will be used in the execution stage
//		key := MakeFragmentKey(queryID, newFragment.FragmentID)
//		(*parent).(*Remote).FragmentKey = key
//
//		//isLeaf = false
//		return false, false
//	})
//
//	if fragments != nil {
//		return fragments, nil
//	} else {
//		// for non-query API calls
//		// --> treat as root fragment and immediately return the result
//		return []Fragment{{
//			Node:       node,
//			FragmentID: uint64(0),
//			ChildIDs:   []uint64{},
//			IsRoot:     true,
//		}}, nil
//	}
//}
