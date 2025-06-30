package compactor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

const (
	MetricLabelName = "__name__"
	MetricName      = "test_metric"
	TestLabelName   = "test_label"
	ConstLabelName  = "const_label"
	ConstLabelValue = "const_value"
)

func TestShardPostingAndSymbolBasedOnPartitionIDAndMetricNamePartitionID(t *testing.T) {
	partitionCount := 8
	metricNamePartitionCount := 4

	tmpdir, err := os.MkdirTemp("", "sharded_posting_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(tmpdir))
	})

	r := rand.New(rand.NewSource(0))
	var series []labels.Labels
	expectedSymbols := make(map[string]bool)
	expectedSymbols[MetricLabelName] = false
	expectedSymbols[ConstLabelName] = false
	expectedSymbols[ConstLabelValue] = false
	expectedSeriesCount := 10
	expectedMetricNameCount := 10
	for i := 0; i < expectedMetricNameCount; i++ {
		name := fmt.Sprintf("%s_%d", MetricName, i)
		expectedSymbols[name] = false
		for j := 0; j < expectedSeriesCount; j++ {
			labelValue := strconv.Itoa(r.Int())
			series = append(series, labels.FromStrings(MetricLabelName, name, ConstLabelName, ConstLabelValue, TestLabelName, labelValue))
			expectedSymbols[TestLabelName] = false
			expectedSymbols[labelValue] = false
		}
	}
	blockID, err := e2eutil.CreateBlock(context.Background(), tmpdir, series, 10, time.Now().Add(-10*time.Minute).UnixMilli(), time.Now().UnixMilli(), labels.EmptyLabels(), 0, metadata.NoneFunc, nil)
	require.NoError(t, err)

	var closers []io.Closer
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()
	seriesCount := 0
	for metricNamePartitionID := 0; metricNamePartitionID < metricNamePartitionCount; metricNamePartitionID++ {
		for partitionID := 0; partitionID < partitionCount; partitionID++ {
			ir, err := index.NewFileReader(filepath.Join(tmpdir, blockID.String(), "index"), index.DecodePostingsRaw)
			closers = append(closers, ir)
			require.NoError(t, err)
			k, v := index.AllPostingsKey()
			postings, err := ir.Postings(context.Background(), k, v)
			require.NoError(t, err)
			postings = ir.SortedPostings(postings)
			blockPartitionInfo := make(map[ulid.ULID]tsdb.BlockPartitionInfo)
			shardedPostings, syms, err := NewShardedPosting(context.Background(), postings, blockID, blockPartitionInfo, ir.Symbols, uint64(partitionCount), uint64(partitionID), uint64(metricNamePartitionCount), uint64(metricNamePartitionID), ir.Series)
			require.NoError(t, err)
			bufChks := make([]chunks.Meta, 0)
			expectedShardedSymbols := make(map[string]struct{})
			for shardedPostings.Next() {
				var builder labels.ScratchBuilder
				err = ir.Series(shardedPostings.At(), &builder, &bufChks)
				require.NoError(t, err)
				require.Equal(t, uint64(partitionID), builder.Labels().Hash()%uint64(partitionCount))
				require.Equal(t, uint64(metricNamePartitionID), hash(builder.Labels().Get(labels.MetricName))%uint64(metricNamePartitionCount))
				seriesCount++
				builder.Labels().Range(func(l labels.Label) {
					expectedShardedSymbols[l.Name] = struct{}{}
					expectedShardedSymbols[l.Value] = struct{}{}
				})
			}
			err = ir.Close()
			if err == nil {
				closers = closers[0 : len(closers)-1]
			}
			symbolsCount := 0
			for s := range syms {
				symbolsCount++
				_, ok := expectedSymbols[s]
				require.True(t, ok)
				expectedSymbols[s] = true
				_, ok = expectedShardedSymbols[s]
				require.True(t, ok)
			}
			require.Equal(t, len(expectedShardedSymbols), symbolsCount)
		}
	}
	require.Equal(t, expectedSeriesCount*expectedMetricNameCount, seriesCount)
	for _, visited := range expectedSymbols {
		require.True(t, visited)
	}
}

func TestShardPostingWithCountSetToOne(t *testing.T) {
	posting := index.NewListPostings([]storage.SeriesRef{1})
	mockLabelFunc := func(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
		return errors.New("label function called")
	}
	mockSymbolIterFunc := func() index.StringIter {
		return index.NewStringListIter([]string{"a"})
	}
	id := ulid.MustNew(1, nil)
	m := map[ulid.ULID]tsdb.BlockPartitionInfo{
		id: {PartitionCount: 16, MetricNamePartitionCount: 8},
	}
	p, symbolMap, err := NewShardedPosting(context.Background(), posting, id, m, mockSymbolIterFunc, 4, 0, 8, 0, mockLabelFunc)
	require.NoError(t, err)
	for p.Next() {
		val := p.At()
		require.Equal(t, storage.SeriesRef(1), val)
	}
	require.NoError(t, p.Err())
	require.Len(t, symbolMap, 1)
	for sym := range symbolMap {
		require.Equal(t, "a", sym)
	}
}
