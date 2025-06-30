package ingester

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/querier/series"
)

func TestShardByMetricNameInfoFromBlockMeta(t *testing.T) {
	for _, tc := range []struct {
		name               string
		meta               tsdb.BlockMeta
		expectedOK         bool
		expectedShardCount uint64
		expectedShardIdx   uint64
	}{
		{
			name:       "no hints",
			meta:       tsdb.BlockMeta{},
			expectedOK: false,
		},
		{
			name: "out of order hint",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{tsdb.CompactionHintFromOutOfOrder},
				},
			},
			expectedOK: false,
		},
		{
			name: "unexpected hints",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{"test", "foo", "bar"},
				},
			},
			expectedOK: false,
		},
		{
			name: "unexpected hints",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{"test", "foo", "bar"},
				},
			},
			expectedOK: false,
		},
		{
			name: "failed to parse",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{metricNameShardInfo},
				},
			},
			expectedOK: false,
		},
		{
			name: "failed to parse, expect both shard count and index",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{fmt.Sprintf("%s=1", metricNameShardInfo)},
				},
			},
			expectedOK: false,
		},
		{
			name: "invalid format",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{fmt.Sprintf("%s=0-2", metricNameShardInfo)},
				},
			},
			expectedOK: false,
		},
		{
			name: "invalid format, multiple _",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{fmt.Sprintf("%s=0_2_3", metricNameShardInfo)},
				},
			},
			expectedOK: false,
		},
		{
			name: "able to parse",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{fmt.Sprintf("%s=0_2", metricNameShardInfo)},
				},
			},
			expectedOK:         true,
			expectedShardIdx:   0,
			expectedShardCount: 2,
		},
		{
			name: "able to parse, contain out of order hints as well",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{
						tsdb.CompactionHintFromOutOfOrder,
						fmt.Sprintf("%s=0_2", metricNameShardInfo),
					},
				},
			},
			expectedOK:         true,
			expectedShardIdx:   0,
			expectedShardCount: 2,
		},
		{
			name: "able to parse, multiple metric name shard info, choose the first one",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{
						fmt.Sprintf("%s=1_3", metricNameShardInfo),
						fmt.Sprintf("%s=0_2", metricNameShardInfo),
					},
				},
			},
			expectedOK:         true,
			expectedShardIdx:   1,
			expectedShardCount: 3,
		},
		{
			name: "fail to parse, negative shard count and index",
			meta: tsdb.BlockMeta{
				Compaction: tsdb.BlockMetaCompaction{
					Hints: []string{
						fmt.Sprintf("%s=-1_-3", metricNameShardInfo),
					},
				},
			},
			expectedOK: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok, shardCount, shardIdx := shardByMetricNameInfoFromBlockMeta(tc.meta)
			require.Equal(t, tc.expectedOK, ok)
			require.Equal(t, tc.expectedShardCount, shardCount)
			require.Equal(t, tc.expectedShardIdx, shardIdx)
		})
	}
}

func TestShardByMetricNameQuerier(t *testing.T) {
	ctx := context.Background()
	dummyCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dummy_counter",
		Help: "dummy counter",
	})
	for _, tc := range []struct {
		name           string
		shardCount     uint64
		shardIdx       uint64
		matchers       []*labels.Matcher
		ms             []model.Metric
		expectedLabels []labels.Labels
	}{
		{
			name: "empty matchers, use default querier",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   nil,
			shardIdx:   0,
			shardCount: 1,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
		{
			name: "shard count == 0, use default querier",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")},
			shardIdx:   0,
			shardCount: 0,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
		{
			name: "shard count == 1",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")},
			shardIdx:   0,
			shardCount: 1,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
		{
			name: "shard count == 3, series doesn't match the shard",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")},
			shardIdx:       0,
			shardCount:     3,
			expectedLabels: []labels.Labels{},
		},
		{
			name: "matchers don't contain metric name, use default querier",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "test")},
			shardIdx:   0,
			shardCount: 3,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
		{
			name: "matchers don't use equal matcher, use default querier",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", "test")},
			shardIdx:   0,
			shardCount: 3,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mq := &storage.MockQuerier{
				SelectMockFunction: func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
					return series.MetricsToSeriesSet(ctx, true, tc.ms)
				},
			}
			q := NewShardByMetricNameQuerier(mq, tc.shardCount, tc.shardIdx, dummyCounter, dummyCounter)
			ss := q.Select(ctx, true, nil, tc.matchers...)
			lbls := make([]labels.Labels, 0)
			for ss.Next() {
				lbls = append(lbls, ss.At().Labels())
			}
			err := ss.Err()
			require.NoError(t, err)
			require.Equal(t, tc.expectedLabels, lbls)
			require.NoError(t, q.Close())
		})
	}
}

func TestShardByMetricNameChunkQuerier(t *testing.T) {
	ctx := context.Background()
	dummyCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dummy_counter",
		Help: "dummy counter",
	})
	for _, tc := range []struct {
		name           string
		shardCount     uint64
		shardIdx       uint64
		matchers       []*labels.Matcher
		ms             []model.Metric
		expectedLabels []labels.Labels
	}{
		{
			name: "empty matchers, use default querier",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   nil,
			shardIdx:   0,
			shardCount: 1,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
		{
			name: "shard count == 0, use default querier",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")},
			shardIdx:   0,
			shardCount: 0,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
		{
			name: "shard count == 1",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")},
			shardIdx:   0,
			shardCount: 1,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
		{
			name: "shard count == 3, series doesn't match the shard",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")},
			shardIdx:       0,
			shardCount:     3,
			expectedLabels: []labels.Labels{},
		},
		{
			name: "matchers don't contain metric name, use default querier",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "test")},
			shardIdx:   0,
			shardCount: 3,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
		{
			name: "matchers don't use equal matcher, use default querier",
			ms: []model.Metric{
				map[model.LabelName]model.LabelValue{
					"test": "a",
				},
			},
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", "test")},
			shardIdx:   0,
			shardCount: 3,
			expectedLabels: []labels.Labels{
				labels.FromMap(map[string]string{"test": "a"}),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mq := &MockChunkQuerier{lbls: tc.ms}
			q := NewShardByMetricNameChunkQuerier(mq, tc.shardCount, tc.shardIdx, dummyCounter, dummyCounter)
			ss := q.Select(ctx, true, nil, tc.matchers...)
			lbls := make([]labels.Labels, 0)
			for ss.Next() {
				lbls = append(lbls, ss.At().Labels())
			}
			err := ss.Err()
			require.NoError(t, err)
			require.Equal(t, tc.expectedLabels, lbls)
			require.NoError(t, q.Close())
		})
	}
}

type MockChunkQuerier struct {
	lbls []model.Metric
}

func (q *MockChunkQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *MockChunkQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *MockChunkQuerier) Close() error {
	return nil
}

func (q *MockChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	return storage.NewSeriesSetToChunkSet(series.MetricsToSeriesSet(ctx, sortSeries, q.lbls))
}
