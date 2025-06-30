package ingester

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestShardByMetricNameCompactor(t *testing.T) {
	logger := log.NewNopLogger()
	slogger := promslog.NewNopLogger()
	ctx := context.Background()
	user := "fake"
	chunkPool := chunkenc.NewPool()
	dir := t.TempDir()
	lbls := make([]labels.Labels, 0)
	totalSeries := 20
	for i := 0; i < totalSeries; i++ {
		lbls = append(lbls, labels.FromStrings("__name__", fmt.Sprintf("test_%d", i), "job", "test"))
	}
	id, err := e2eutil.CreateBlock(ctx, dir, lbls, 5, 0, 1000, labels.EmptyLabels(), 0, metadata.NoneFunc, nil)
	require.NoError(t, err)

	builder := labels.NewScratchBuilder(0)
	for _, tc := range []struct {
		name           string
		shardSize      int
		expectedBlocks int
	}{
		{
			name:           "shard size 0",
			shardSize:      0,
			expectedBlocks: 1,
		},
		{
			name:           "shard size 1",
			shardSize:      1,
			expectedBlocks: 1,
		},
		{
			name:           "shard size 2",
			shardSize:      2,
			expectedBlocks: 2,
		},
		{
			name:           "shard size 4",
			shardSize:      4,
			expectedBlocks: 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits := defaultLimitsTestConfig()
			limits.MetricNameShardSize = tc.shardSize
			overrides := validation.NewOverrides(limits, nil)
			require.NoError(t, err)
			r := prometheus.NewRegistry()
			c := &ShardByMetricNameCompactor{
				ctx:                      ctx,
				logger:                   logger,
				slogger:                  slogger,
				userID:                   user,
				chunkPool:                chunkPool,
				metrics:                  tsdb.NewCompactorMetrics(r),
				maxBlockChunkSegmentSize: chunks.DefaultChunkSegmentSize,
				overrides:                overrides,
			}

			if tc.shardSize == 0 {
				tc.shardSize = 1
			}
			b, err := tsdb.OpenBlock(slogger, path.Join(dir, id.String()), chunkPool, nil)
			require.NoError(t, err)
			defer b.Close()
			samplesBefore := b.Meta().Stats.NumSamples
			ulids, err := c.Write(dir, b, 0, 1000, nil)
			require.NoError(t, err)
			require.Len(t, ulids, tc.expectedBlocks)
			seriesCount := 0
			var samplesAfter uint64
			for i, ulid := range ulids {
				meta, err := metadata.ReadFromDir(path.Join(dir, ulid.String()))
				require.NoError(t, err)
				require.Equal(t, []string{fmt.Sprintf("%s=%d_%d", metricNameShardInfo, i, tc.shardSize)}, meta.Compaction.Hints)
				cortexExtension, err := cortex_tsdb.GetCortexMetaExtensionsFromMeta(*meta)
				require.NoError(t, err)
				require.Equal(t, cortexExtension.PartitionInfo, &cortex_tsdb.PartitionInfo{
					PartitionCount:           1,
					PartitionID:              0,
					MetricNamePartitionCount: tc.shardSize,
					MetricNamePartitionID:    i,
				})
				require.Equal(t, cortexExtension.Version, cortex_tsdb.CortexMetaExtensionsVersion1)

				b, err = tsdb.OpenBlock(slogger, path.Join(dir, ulid.String()), chunkPool, nil)
				require.NoError(t, err)
				samplesAfter += b.Meta().Stats.NumSamples

				ir, err := b.Index()
				require.NoError(t, err)
				p := tsdb.AllSortedPostings(ctx, ir)
				iters := 0
				builder.Reset()
				for p.Next() {
					iters++
					ref := p.At()
					err = ir.Series(ref, &builder, nil)
					require.NoError(t, err)

					// Check block has partitioned series.
					metricName := builder.Labels().Get(labels.MetricName)
					require.Equal(t, uint64(i), hash(metricName)%uint64(tc.shardSize))
				}
				require.NoError(t, p.Err())
				seriesCount += iters
				require.NoError(t, ir.Close())
				require.NoError(t, b.Close())
			}

			// Make sure we have total number of series correct.
			require.Equal(t, totalSeries, seriesCount)
			// Make sure samples before and after compaction is correct.
			require.Equal(t, samplesBefore, samplesAfter)
		})
	}
}

func TestShardByMetricNameCompactorHeadCompaction(t *testing.T) {
	logger := log.NewNopLogger()
	slogger := promslog.NewNopLogger()
	ctx := context.Background()
	user := "fake"
	chunkPool := chunkenc.NewPool()
	dir := t.TempDir()

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = 10000000000
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	require.NoError(t, err)
	defer h.Close()
	app := h.Appender(ctx)
	totalSeries := 20
	samplesPerSeries := 5
	for i := 0; i < totalSeries; i++ {
		var (
			ts  int64
			ref storage.SeriesRef
		)
		lbl := labels.FromStrings("__name__", fmt.Sprintf("test_%d", i), "job", "test")
		for j := 0; j < samplesPerSeries; j++ {
			ref, err = app.Append(ref, lbl, ts, float64(j))
			require.NoError(t, err)
			ts += 200
		}
	}
	require.NoError(t, app.Commit())
	rh := tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime())

	expectedBlocks := 4
	limits := defaultLimitsTestConfig()
	limits.MetricNameShardSize = expectedBlocks
	overrides := validation.NewOverrides(limits, nil)
	require.NoError(t, err)
	r := prometheus.NewRegistry()
	c := &ShardByMetricNameCompactor{
		ctx:                      ctx,
		logger:                   logger,
		slogger:                  slogger,
		userID:                   user,
		chunkPool:                chunkPool,
		metrics:                  tsdb.NewCompactorMetrics(r),
		maxBlockChunkSegmentSize: chunks.DefaultChunkSegmentSize,
		overrides:                overrides,
	}

	ulids, err := c.Write(dir, rh, 0, 1000, nil)
	require.NoError(t, err)
	require.Len(t, ulids, expectedBlocks)
	seriesCount := 0
	builder := labels.ScratchBuilder{}
	for i, ulid := range ulids {
		meta, err := metadata.ReadFromDir(path.Join(dir, ulid.String()))
		require.NoError(t, err)
		require.Equal(t, []string{fmt.Sprintf("%s=%d_%d", metricNameShardInfo, i, expectedBlocks)}, meta.Compaction.Hints)
		cortexExtension, err := cortex_tsdb.GetCortexMetaExtensionsFromMeta(*meta)
		require.NoError(t, err)
		require.Equal(t, cortexExtension.PartitionInfo, &cortex_tsdb.PartitionInfo{
			PartitionCount:           1,
			PartitionID:              0,
			MetricNamePartitionCount: expectedBlocks,
			MetricNamePartitionID:    i,
		})
		require.Equal(t, cortexExtension.Version, cortex_tsdb.CortexMetaExtensionsVersion1)

		b, err := tsdb.OpenBlock(slogger, path.Join(dir, ulid.String()), chunkPool, nil)
		require.NoError(t, err)

		ir, err := b.Index()
		require.NoError(t, err)
		p := tsdb.AllSortedPostings(ctx, ir)
		iters := 0
		builder.Reset()
		for p.Next() {
			iters++
			ref := p.At()
			err = ir.Series(ref, &builder, nil)
			require.NoError(t, err)

			// Check block has partitioned series.
			metricName := builder.Labels().Get(labels.MetricName)
			require.Equal(t, uint64(i), hash(metricName)%uint64(expectedBlocks))
		}
		require.NoError(t, p.Err())
		seriesCount += iters
		require.NoError(t, ir.Close())
		require.NoError(t, b.Close())
	}

	// Make sure we have total number of series correct.
	require.Equal(t, totalSeries, seriesCount)
}
