package ingester

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	indexFilename                = "index"
	tmpForCreationBlockDirSuffix = ".tmp-for-creation"

	metricNameShardInfo = "metric_name_shard_info"
)

func chunkDir(dir string) string { return filepath.Join(dir, "chunks") }

// write creates a new block that is the union of the provided blocks into dir.
func write(ctx context.Context, metrics *tsdb.CompactorMetrics, logger log.Logger, slogger *slog.Logger, chunkPool chunkenc.Pool, maxBlockChunkSegmentSize int64, dest string, meta *metadata.Meta, blockPopulator tsdb.BlockPopulator, postingsFunc tsdb.IndexReaderPostingsFunc, blocks ...tsdb.BlockReader) (err error) {
	dir := filepath.Join(dest, meta.ULID.String())
	tmp := dir + tmpForCreationBlockDirSuffix
	var closers []io.Closer
	defer func() {
		err = tsdb_errors.NewMulti(err, tsdb_errors.CloseAll(closers)).Err()

		// RemoveAll returns no error when tmp doesn't exist so it is safe to always run it.
		if err := os.RemoveAll(tmp); err != nil {
			level.Error(logger).Log("msg", "removed tmp folder after failed compaction", "err", err.Error())
		}
	}()

	if err = os.RemoveAll(tmp); err != nil {
		return err
	}

	if err = os.MkdirAll(tmp, 0o777); err != nil {
		return err
	}

	// Populate chunk and index files into temporary directory with
	// data of all blocks.
	var chunkw tsdb.ChunkWriter

	chunkw, err = chunks.NewWriter(chunkDir(tmp), chunks.WithSegmentSize(maxBlockChunkSegmentSize))
	if err != nil {
		return fmt.Errorf("open chunk writer: %w", err)
	}
	closers = append(closers, chunkw)
	// Record written chunk sizes on level 1 compactions.
	if meta.Compaction.Level == 1 {
		chunkw = &instrumentedChunkWriter{
			ChunkWriter: chunkw,
			size:        metrics.ChunkSize,
			samples:     metrics.ChunkSamples,
			trange:      metrics.ChunkRange,
		}
	}

	indexw, err := index.NewWriterWithEncoder(ctx, filepath.Join(tmp, indexFilename), index.EncodePostingsRaw)
	if err != nil {
		return fmt.Errorf("open index writer: %w", err)
	}
	closers = append(closers, indexw)

	if err := blockPopulator.PopulateBlock(ctx, metrics, slogger, chunkPool, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge), blocks, &meta.BlockMeta, indexw, chunkw, postingsFunc); err != nil {
		return fmt.Errorf("populate block: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// We are explicitly closing them here to check for error even
	// though these are covered under defer. This is because in Windows,
	// you cannot delete these unless they are closed and the defer is to
	// make sure they are closed if the function exits due to an error above.
	errs := tsdb_errors.NewMulti()
	for _, w := range closers {
		errs.Add(w.Close())
	}
	closers = closers[:0] // Avoid closing the writers twice in the defer.
	if errs.Err() != nil {
		return errs.Err()
	}

	// Populated block is empty, so exit early.
	if meta.Stats.NumSamples == 0 {
		return nil
	}

	if err := meta.WriteToDir(logger, tmp); err != nil {
		return fmt.Errorf("write merged meta: %w", err)
	}

	df, err := fileutil.OpenDir(tmp)
	if err != nil {
		return fmt.Errorf("open temporary block dir: %w", err)
	}
	defer func() {
		if df != nil {
			df.Close()
		}
	}()

	if err := df.Sync(); err != nil {
		return fmt.Errorf("sync temporary dir file: %w", err)
	}

	// Close temp dir before rename block dir (for windows platform).
	if err = df.Close(); err != nil {
		return fmt.Errorf("close temporary dir: %w", err)
	}
	df = nil

	// Block successfully written, make it visible in destination dir by moving it from tmp one.
	if err := fileutil.Replace(tmp, dir); err != nil {
		return fmt.Errorf("rename block dir: %w", err)
	}

	return nil
}

// instrumentedChunkWriter is used for level 1 compactions to record statistics
// about compacted chunks.
type instrumentedChunkWriter struct {
	tsdb.ChunkWriter

	size    prometheus.Histogram
	samples prometheus.Histogram
	trange  prometheus.Histogram
}

func (w *instrumentedChunkWriter) WriteChunks(chunks ...chunks.Meta) error {
	for _, c := range chunks {
		w.size.Observe(float64(len(c.Chunk.Bytes())))
		w.samples.Observe(float64(c.Chunk.NumSamples()))
		w.trange.Observe(float64(c.MaxTime - c.MinTime))
	}
	return w.ChunkWriter.WriteChunks(chunks...)
}

type ShardByMetricNameCompactor struct {
	ctx       context.Context
	logger    log.Logger
	slogger   *slog.Logger
	userID    string
	overrides *validation.Overrides

	chunkPool                chunkenc.Pool
	maxBlockChunkSegmentSize int64
	metrics                  *tsdb.CompactorMetrics
}

// Plan is a noop since we disable compaction in Ingester.
func (c *ShardByMetricNameCompactor) Plan(dir string) ([]string, error) {
	return nil, nil
}

// Compact is a noop since we disable compaction in Ingester.
func (c *ShardByMetricNameCompactor) Compact(dest string, dirs []string, open []*tsdb.Block) ([]ulid.ULID, error) {
	return nil, nil
}

func (c *ShardByMetricNameCompactor) Write(dest string, b tsdb.BlockReader, mint, maxt int64, parent *tsdb.BlockMeta) ([]ulid.ULID, error) {
	shardSize := c.overrides.GetMetricNameShardSize(c.userID)
	if shardSize == 0 {
		shardSize = 1
	}
	postingFuncs := make([]tsdb.IndexReaderPostingsFunc, shardSize)
	// Validate we have the same amount of samples before and after compaction.
	var samplesBefore uint64

	// Shortcut.
	if shardSize == 1 {
		postingFuncs[0] = tsdb.AllSortedPostings
	} else {
		cr, err := b.Chunks()
		if err != nil {
			return nil, err
		}
		defer cr.Close()
		for i := 0; i < shardSize; i++ {
			i := i
			pf := func(ctx context.Context, reader tsdb.IndexReader) index.Postings {
				k, v := index.AllPostingsKey()
				allPostings, err := reader.Postings(ctx, k, v)
				if err != nil {
					return index.ErrPostings(err)
				}
				var builder labels.ScratchBuilder
				var chks []chunks.Meta
				postings := []storage.SeriesRef{}
				for allPostings.Next() {
					ref := allPostings.At()
					if err := reader.Series(ref, &builder, &chks); err != nil {
						return index.ErrPostings(err)
					}
					if hash(builder.Labels().Get(labels.MetricName))%uint64(shardSize) == uint64(i) {
						postings = append(postings, ref)

						// Head doesn't have number of samples in metadata stats
						// so we have to calculate from chunks.
						for _, chk := range chks {
							c, _, err := cr.ChunkOrIterable(chk)
							if err != nil && !errors.Is(err, storage.ErrNotFound) {
								return index.ErrPostings(err)
							}
							// It is possible for c to be nil when it is an out of order
							// chunk. We ignore that case for now.
							if c != nil {
								samplesBefore += uint64(c.NumSamples())
							}
						}
					}
				}
				return reader.SortedPostings(index.NewListPostings(postings))
			}
			postingFuncs[i] = pf
		}
	}

	var ooo bool
	uids := make([]ulid.ULID, 0, len(postingFuncs))
	defer func(t time.Time) {
		c.metrics.Ran.Inc()
		duration := time.Since(t).Seconds()
		c.metrics.Duration.Observe(duration)
		level.Info(c.logger).Log(
			"msg", "finish writing blocks",
			"blocks", len(uids),
			"duration", duration,
			"ooo", ooo,
		)
	}(time.Now())

	var samplesAfter uint64
	for i, postingFunc := range postingFuncs {
		start := time.Now()

		uid := ulid.MustNew(ulid.Now(), rand.Reader)

		meta := &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				Version: metadata.TSDBVersion1,
				ULID:    uid,
				MinTime: mint,
				MaxTime: maxt,
				Compaction: tsdb.BlockMetaCompaction{
					Level:   1,
					Sources: []ulid.ULID{uid},
				},
			},
			Thanos: metadata.Thanos{
				Extensions: &cortex_tsdb.CortexMetaExtensions{
					PartitionInfo: &cortex_tsdb.PartitionInfo{
						MetricNamePartitionCount: shardSize,
						MetricNamePartitionID:    i,
						// Set partition count and id to default value.
						PartitionCount: 1,
						PartitionID:    0,
					},
					Version: cortex_tsdb.CortexMetaExtensionsVersion1,
				},
			},
		}
		meta.Compaction.Hints = []string{metricNameShardInfo + "=" + fmt.Sprintf("%d_%d", i, shardSize)}

		if parent != nil {
			meta.Compaction.Parents = []tsdb.BlockDesc{
				{ULID: parent.ULID, MinTime: parent.MinTime, MaxTime: parent.MaxTime},
			}
			if parent.Compaction.FromOutOfOrder() {
				meta.Compaction.SetOutOfOrder()
				ooo = true
			}
		}

		err := write(c.ctx, c.metrics, c.logger, c.slogger, c.chunkPool, c.maxBlockChunkSegmentSize, dest, meta, tsdb.DefaultBlockPopulator{}, postingFunc, b)
		if err != nil {
			return nil, err
		}

		samplesAfter += meta.Stats.NumSamples
		if meta.Stats.NumSamples == 0 {
			level.Info(c.logger).Log(
				"msg", "write block resulted in empty block",
				"mint", meta.MinTime,
				"maxt", meta.MaxTime,
				"duration", time.Since(start),
			)
			continue
		}

		uids = append(uids, uid)
		level.Info(c.logger).Log(
			"msg", "write block",
			"mint", meta.MinTime,
			"maxt", meta.MaxTime,
			"ulid", meta.ULID,
			"duration", time.Since(start),
			"ooo", meta.Compaction.FromOutOfOrder(),
		)
	}

	// TODO: remove samples validation when it is stable.
	if samplesBefore > 0 && samplesBefore != samplesAfter {
		// Cleanup compacted blocks.
		multiErr := tsdb_errors.NewMulti(fmt.Errorf("number of samples mismatch before and after head compaction, before: %d, after: %d", samplesBefore, samplesAfter))
		for _, uid := range uids {
			if errRemoveAll := os.RemoveAll(filepath.Join(dest, uid.String())); errRemoveAll != nil {
				multiErr.Add(fmt.Errorf("delete persisted head block after failed samples count validation:%s: %w", uid, errRemoveAll))
			}
		}
		return nil, multiErr.Err()
	}

	return uids, nil
}

func hash(s string) uint64 {
	h := xxhash.New()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
