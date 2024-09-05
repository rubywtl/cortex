package ingester

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
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

type mockBReader struct {
	ir tsdb.IndexReader
	cr tsdb.ChunkReader
}

func (r *mockBReader) Index() (tsdb.IndexReader, error)  { return r.ir, nil }
func (r *mockBReader) Chunks() (tsdb.ChunkReader, error) { return r.cr, nil }
func (r *mockBReader) Tombstones() (tombstones.Reader, error) {
	return tombstones.NewMemTombstones(), nil
}
func (r *mockBReader) Meta() tsdb.BlockMeta { return tsdb.BlockMeta{MinTime: 0, MaxTime: 1000} }
func (r *mockBReader) Size() int64          { return 0 }

type mockChunkReader struct {
	emptyChunk chunkenc.Chunk
}

func (cr mockChunkReader) ChunkOrIterable(chunks.Meta) (chunkenc.Chunk, chunkenc.Iterable, error) {
	return cr.emptyChunk, nil, nil
}

func (cr mockChunkReader) Close() error { return nil }

type mockIndexReaderWithFunc struct {
	postingsFunc func() index.Postings
	seriesFunc   func(builder *labels.ScratchBuilder, chks *[]chunks.Meta) error
}

func (ir mockIndexReaderWithFunc) Symbols() index.StringIter {
	return index.NewStringListIter([]string{})
}

func (ir mockIndexReaderWithFunc) SortedLabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (ir mockIndexReaderWithFunc) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (ir mockIndexReaderWithFunc) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	return ir.postingsFunc(), nil
}

func (ir mockIndexReaderWithFunc) PostingsForLabelMatching(ctx context.Context, name string, match func(value string) bool) index.Postings {
	return nil
}

func (ir mockIndexReaderWithFunc) SortedPostings(p index.Postings) index.Postings {
	return p
}

func (ir mockIndexReaderWithFunc) ShardedPostings(p index.Postings, shardIndex, shardCount uint64) index.Postings {
	return nil
}

func (ir mockIndexReaderWithFunc) Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
	return ir.seriesFunc(builder, chks)
}

func (ir mockIndexReaderWithFunc) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (ir mockIndexReaderWithFunc) LabelValueFor(ctx context.Context, id storage.SeriesRef, label string) (string, error) {
	return "", nil
}

func (ir mockIndexReaderWithFunc) LabelNamesFor(ctx context.Context, postings index.Postings) ([]string, error) {
	return nil, nil
}

func (ir mockIndexReaderWithFunc) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	return nil
}

func (ir mockIndexReaderWithFunc) Close() error { return nil }

func TestShardByMetricNameCompactorSeriesNotFound(t *testing.T) {
	logger := log.NewNopLogger()
	slogger := promslog.NewNopLogger()
	ctx := context.Background()
	user := "fake"
	chunkPool := chunkenc.NewPool()
	dir := t.TempDir()
	expectedBlocks := 2
	limits := defaultLimitsTestConfig()
	limits.MetricNameShardSize = expectedBlocks
	overrides := validation.NewOverrides(limits, nil)
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
	// Index reader should always return series not found.
	ir := mockIndexReaderWithFunc{
		postingsFunc: func() index.Postings {
			return index.NewListPostings([]storage.SeriesRef{storage.SeriesRef(1)})
		},
		seriesFunc: func(builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
			return storage.ErrNotFound
		},
	}
	// We expect series not found error to be ignored.
	_, err := c.Write(dir, &mockBReader{ir: ir, cr: mockChunkReader{}}, 0, 1000, nil)
	require.NoError(t, err)
}

func TestShardByMetricNameCompactorPostingIterError(t *testing.T) {
	logger := log.NewNopLogger()
	slogger := promslog.NewNopLogger()
	ctx := context.Background()
	user := "fake"
	chunkPool := chunkenc.NewPool()
	dir := t.TempDir()
	expectedBlocks := 2
	limits := defaultLimitsTestConfig()
	limits.MetricNameShardSize = expectedBlocks
	overrides := validation.NewOverrides(limits, nil)
	r := prometheus.NewRegistry()

	postingErr := errors.New("posting error")
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
	// Index reader should always return series not found.
	ir := mockIndexReaderWithFunc{
		postingsFunc: func() index.Postings {
			return index.ErrPostings(postingErr)
		},
		seriesFunc: func(builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
			return storage.ErrNotFound
		},
	}
	// We expect series not found error to be ignored.
	_, err := c.Write(dir, &mockBReader{ir: ir, cr: mockChunkReader{}}, 0, 1000, nil)
	require.Error(t, err)
	require.Equal(t, fmt.Errorf("iterate compaction set: %w", errors.Wrap(postingErr, "iterate postings")).Error(), err.Error())
}
