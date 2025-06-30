package compactor

import (
	"context"

	"github.com/cespare/xxhash/v2"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
)

func NewShardedPosting(ctx context.Context, postings index.Postings, id ulid.ULID, blockPartitionInfos map[ulid.ULID]tsdb.BlockPartitionInfo, symbolIterFunc func() index.StringIter, partitionCount, partitionID, metricNamePartitionCount, metricNamePartitionID uint64, labelsFn func(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error) (index.Postings, map[string]struct{}, error) {
	symbols := make(map[string]struct{})

	blockPartitionInfo, ok := blockPartitionInfos[id]
	if ok {
		// The block was originally partitioned with more partitions. No need to filter series further.
		// Overwrite to one means skipping all labels partitioning.
		if uint64(blockPartitionInfo.PartitionCount) >= partitionCount {
			partitionCount = 1
		}
		// The block was originally partitioned with more metric partitions. No need to filter series further.
		// Overwrite to one means skipping metric name partitioning.
		if uint64(blockPartitionInfo.MetricNamePartitionCount) >= metricNamePartitionCount {
			metricNamePartitionCount = 1
		}
	}

	// Skip series partitioning if both partition dimension set to 1.
	if metricNamePartitionCount == 1 && partitionCount == 1 {
		symbolIter := symbolIterFunc()
		for symbolIter.Next() {
			symbols[symbolIter.At()] = struct{}{}
		}
		return postings, symbols, symbolIter.Err()
	}

	series := make([]storage.SeriesRef, 0)
	var builder labels.ScratchBuilder
	cnt := 0
	for postings.Next() {
		cnt++
		if cnt%util.CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		err := labelsFn(postings.At(), &builder, nil)
		if err != nil {
			return nil, nil, err
		}
		lbls := builder.Labels()
		if metricNamePartitionCount > 1 {
			if hash(lbls.Get(labels.MetricName))%metricNamePartitionCount != metricNamePartitionID {
				continue
			}
		}
		if partitionCount > 1 {
			if lbls.Hash()%partitionCount != partitionID {
				continue
			}
		}

		posting := postings.At()
		series = append(series, posting)
		lbls.Range(func(l labels.Label) {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		})
	}
	return index.NewListPostings(series), symbols, nil
}

func hash(s string) uint64 {
	h := xxhash.New()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
