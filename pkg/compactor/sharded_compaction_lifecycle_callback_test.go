package compactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestPreCompactionCallback(t *testing.T) {
	compactDir, err := os.MkdirTemp(os.TempDir(), "compact")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(compactDir))
	})

	lifecycleCallback := ShardedCompactionLifecycleCallback{
		compactDir: compactDir,
	}

	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	meta := []*metadata.Meta{
		{
			BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
		},
		{
			BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
		},
		{
			BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * time.Hour.Milliseconds(), MaxTime: 3 * time.Hour.Milliseconds()},
		},
	}
	testGroupKey := "test_group_key"
	testGroup, _ := compact.NewGroup(
		log.NewNopLogger(),
		nil,
		testGroupKey,
		labels.EmptyLabels(),
		0,
		true,
		true,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		metadata.NoneFunc,
		1,
		1,
	)
	for _, m := range meta {
		err := testGroup.AppendMeta(m)
		require.NoError(t, err)
	}

	dummyGroupID1 := "dummy_dir_1"
	dummyGroupID2 := "dummy_dir_2"
	err = os.MkdirAll(filepath.Join(compactDir, testGroupKey), 0750)
	require.NoError(t, err)
	err = os.MkdirAll(filepath.Join(compactDir, testGroupKey, block1.String()), 0750)
	require.NoError(t, err)
	err = os.MkdirAll(filepath.Join(compactDir, dummyGroupID1), 0750)
	require.NoError(t, err)
	err = os.MkdirAll(filepath.Join(compactDir, dummyGroupID2), 0750)
	require.NoError(t, err)

	err = lifecycleCallback.PreCompactionCallback(context.Background(), log.NewNopLogger(), testGroup, meta)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(compactDir, testGroupKey))
	require.NoError(t, err)
	require.True(t, info.IsDir())
	info, err = os.Stat(filepath.Join(compactDir, testGroupKey, block1.String()))
	require.NoError(t, err)
	require.True(t, info.IsDir())
	_, err = os.Stat(filepath.Join(compactDir, dummyGroupID1))
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(compactDir, dummyGroupID2))
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}

func TestGetBlockPopulator(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	partitionGroupID := uint32(111)
	partitionCreationTime := int64(999)
	ulid1 := ulid.MustNew(1, nil)
	ulid2 := ulid.MustNew(2, nil)
	ulid3 := ulid.MustNew(3, nil)
	for _, tc := range []struct {
		name              string
		extensions        any
		expectedPopulator tsdb.BlockPopulator
	}{
		{
			name:              "nil extensions",
			extensions:        nil,
			expectedPopulator: tsdb.DefaultBlockPopulator{},
		},
		{
			name: "0 partition count",
			extensions: &cortex_tsdb.CortexMetaExtensions{
				PartitionInfo: &cortex_tsdb.PartitionInfo{
					PartitionCount:               0,
					PartitionedGroupID:           partitionGroupID,
					PartitionedGroupCreationTime: partitionCreationTime,
				},
			},
			expectedPopulator: ShardedBlockPopulator{
				metricNamePartitionCount: 1,
				metricNamePartitionID:    0,
				partitionCount:           1,
				partitionID:              0,
				logger:                   logger,
			},
		},
		{
			name: "2 partition count, but 0 metric name count",
			extensions: &cortex_tsdb.CortexMetaExtensions{
				PartitionInfo: &cortex_tsdb.PartitionInfo{
					PartitionCount:               2,
					PartitionedGroupID:           partitionGroupID,
					PartitionedGroupCreationTime: partitionCreationTime,
				},
			},
			expectedPopulator: ShardedBlockPopulator{
				metricNamePartitionCount: 1,
				metricNamePartitionID:    0,
				partitionCount:           2,
				partitionID:              0,
				logger:                   logger,
			},
		},
		{
			name: "4 partition count, 8 metric name count",
			extensions: &cortex_tsdb.CortexMetaExtensions{
				PartitionInfo: &cortex_tsdb.PartitionInfo{
					PartitionCount:               4,
					MetricNamePartitionCount:     8,
					PartitionedGroupID:           partitionGroupID,
					PartitionedGroupCreationTime: partitionCreationTime,
				},
			},
			expectedPopulator: ShardedBlockPopulator{
				metricNamePartitionCount: 8,
				metricNamePartitionID:    0,
				partitionCount:           4,
				partitionID:              0,
				logger:                   logger,
			},
		},
		{
			name: "4 partition count, 2 partition id, 8 metric name count, 3 metric name partition id",
			extensions: &cortex_tsdb.CortexMetaExtensions{
				PartitionInfo: &cortex_tsdb.PartitionInfo{
					PartitionCount:               4,
					PartitionID:                  2,
					MetricNamePartitionCount:     8,
					MetricNamePartitionID:        3,
					PartitionedGroupID:           partitionGroupID,
					PartitionedGroupCreationTime: partitionCreationTime,
				},
			},
			expectedPopulator: ShardedBlockPopulator{
				metricNamePartitionCount: 8,
				metricNamePartitionID:    3,
				partitionCount:           4,
				partitionID:              2,
				logger:                   logger,
			},
		},
		{
			name: "block partition info exists",
			extensions: &cortex_tsdb.CortexMetaExtensions{
				PartitionInfo: &cortex_tsdb.PartitionInfo{
					PartitionCount:               4,
					PartitionID:                  2,
					MetricNamePartitionCount:     8,
					MetricNamePartitionID:        3,
					PartitionedGroupID:           partitionGroupID,
					PartitionedGroupCreationTime: partitionCreationTime,
					BlockPartitionInfos: map[ulid.ULID]cortex_tsdb.BlockPartitionInfo{
						ulid1: {PartitionCount: 4, MetricNamePartitionCount: 8},
						ulid2: {PartitionCount: 1, MetricNamePartitionCount: 4},
						ulid3: {PartitionCount: 2, MetricNamePartitionCount: 16},
					},
				},
			},
			expectedPopulator: ShardedBlockPopulator{
				metricNamePartitionCount: 8,
				metricNamePartitionID:    3,
				partitionCount:           4,
				partitionID:              2,
				logger:                   logger,
				blockPartitionInfos: map[ulid.ULID]cortex_tsdb.BlockPartitionInfo{
					ulid1: {PartitionCount: 4, MetricNamePartitionCount: 8},
					ulid2: {PartitionCount: 1, MetricNamePartitionCount: 4},
					ulid3: {PartitionCount: 2, MetricNamePartitionCount: 16},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cg := &compact.Group{}
			cg.SetExtensions(tc.extensions)
			c := ShardedCompactionLifecycleCallback{}
			bp, err := c.GetBlockPopulator(ctx, logger, cg)
			require.NoError(t, err)
			require.Equal(t, tc.expectedPopulator, bp)
			ext := cg.Extensions()
			partitionInfo, err := cortex_tsdb.ConvertToPartitionInfo(ext)
			require.NoError(t, err)
			if partitionInfo != nil {
				require.Nil(t, partitionInfo.BlockPartitionInfos)
			}
		})
	}
}
