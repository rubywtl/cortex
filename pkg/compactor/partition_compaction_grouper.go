package compactor

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	thanosblock "github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
)

var (
	DUMMY_BLOCK_ID = ulid.ULID{}
)

type PartitionCompactionGrouper struct {
	ctx                      context.Context
	logger                   log.Logger
	bkt                      objstore.InstrumentedBucket
	acceptMalformedIndex     bool
	enableVerticalCompaction bool
	blocksMarkedForNoCompact prometheus.Counter
	hashFunc                 metadata.HashFunc
	syncerMetrics            *compact.SyncerMetrics
	compactorMetrics         *compactorMetrics
	compactorCfg             Config
	limits                   Limits
	userID                   string
	blockFilesConcurrency    int
	blocksFetchConcurrency   int
	compactionConcurrency    int

	doRandomPick bool

	ring               ring.ReadRing
	ringLifecyclerAddr string
	ringLifecyclerID   string

	noCompBlocksFunc            func() map[ulid.ULID]*metadata.NoCompactMark
	partitionVisitMarkerTimeout time.Duration

	ingestionReplicationFactor int
}

func NewPartitionCompactionGrouper(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.InstrumentedBucket,
	acceptMalformedIndex bool,
	enableVerticalCompaction bool,
	blocksMarkedForNoCompact prometheus.Counter,
	syncerMetrics *compact.SyncerMetrics,
	compactorMetrics *compactorMetrics,
	hashFunc metadata.HashFunc,
	compactorCfg Config,
	ring ring.ReadRing,
	ringLifecyclerAddr string,
	ringLifecyclerID string,
	limits Limits,
	userID string,
	blockFilesConcurrency int,
	blocksFetchConcurrency int,
	compactionConcurrency int,
	doRandomPick bool,
	partitionVisitMarkerTimeout time.Duration,
	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark,
	ingestionReplicationFactor int,
) *PartitionCompactionGrouper {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &PartitionCompactionGrouper{
		ctx:                         ctx,
		logger:                      logger,
		bkt:                         bkt,
		acceptMalformedIndex:        acceptMalformedIndex,
		enableVerticalCompaction:    enableVerticalCompaction,
		blocksMarkedForNoCompact:    blocksMarkedForNoCompact,
		hashFunc:                    hashFunc,
		syncerMetrics:               syncerMetrics,
		compactorMetrics:            compactorMetrics,
		compactorCfg:                compactorCfg,
		ring:                        ring,
		ringLifecyclerAddr:          ringLifecyclerAddr,
		ringLifecyclerID:            ringLifecyclerID,
		limits:                      limits,
		userID:                      userID,
		blockFilesConcurrency:       blockFilesConcurrency,
		blocksFetchConcurrency:      blocksFetchConcurrency,
		compactionConcurrency:       compactionConcurrency,
		doRandomPick:                doRandomPick,
		partitionVisitMarkerTimeout: partitionVisitMarkerTimeout,
		noCompBlocksFunc:            noCompBlocksFunc,
		ingestionReplicationFactor:  ingestionReplicationFactor,
	}
}

// Groups function modified from https://github.com/cortexproject/cortex/pull/2616
func (g *PartitionCompactionGrouper) Groups(blocks map[ulid.ULID]*metadata.Meta) (res []*compact.Group, err error) {
	// Check if this compactor is on the subring.
	// If the compactor is not on the subring when using the userID as a identifier
	// no plans generated below will be owned by the compactor so we can just return an empty array
	// as there will be no planned groups
	onSubring, err := g.checkSubringForCompactor()
	if err != nil {
		return nil, errors.Wrap(err, "unable to check sub-ring for compactor ownership")
	}
	if !onSubring {
		level.Debug(g.logger).Log("msg", "compactor is not on the current sub-ring skipping user", "user", g.userID)
		return nil, nil
	}

	// Filter out no compact blocks
	noCompactMarked := g.noCompBlocksFunc()
	for id, b := range blocks {
		if _, excluded := noCompactMarked[b.ULID]; excluded {
			delete(blocks, id)
		}
	}

	partitionCompactionJobs, err := g.generateCompactionJobs(blocks)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate compaction jobs")
	}

	pickedPartitionCompactionJobs := g.pickPartitionCompactionJob(partitionCompactionJobs)

	return pickedPartitionCompactionJobs, nil
}

// Check whether this compactor exists on the subring based on user ID
func (g *PartitionCompactionGrouper) checkSubringForCompactor() (bool, error) {
	shardSize := util.DynamicShardSize(g.limits.CompactorTenantShardSize(g.userID), g.ring.InstancesCount())
	subRing := g.ring.ShuffleShard(g.userID, shardSize)

	rs, err := subRing.GetAllHealthy(RingOp)
	if err != nil {
		return false, err
	}

	return rs.Includes(g.ringLifecyclerAddr), nil
}

func (g *PartitionCompactionGrouper) generateCompactionJobs(blocks map[ulid.ULID]*metadata.Meta) ([]*blocksGroupWithPartition, error) {
	timeRanges := g.compactorCfg.BlockRanges.ToMilliseconds()

	groups := g.groupBlocks(blocks, timeRanges)

	existingPartitionedGroups, err := g.loadExistingPartitionedGroups()
	if err != nil {
		return nil, err
	}

	var blockIDs []string
	for _, p := range existingPartitionedGroups {
		blockIDs = p.getAllBlockIDs()
		level.Info(g.logger).Log("msg", "existing partitioned group", "partitioned_group_id", p.PartitionedGroupID, "metric_name_partition_count", p.MetricNamePartitionCount, "rangeStart", p.rangeStartTime().String(), "rangeEnd", p.rangeEndTime().String(), "blocks", strings.Join(blockIDs, ","))
	}

	allPartitionedGroup, err := g.generatePartitionedGroups(blocks, groups, existingPartitionedGroups, timeRanges)
	if err != nil {
		return nil, err
	}
	g.sortPartitionedGroups(allPartitionedGroup)
	for _, p := range allPartitionedGroup {
		blockIDs = p.getAllBlockIDs()
		level.Info(g.logger).Log("msg", "partitioned group ready for compaction", "partitioned_group_id", p.PartitionedGroupID, "metric_name_partition_count", p.MetricNamePartitionCount, "rangeStart", p.rangeStartTime().String(), "rangeEnd", p.rangeEndTime().String(), "blocks", strings.Join(blockIDs, ","))
	}

	partitionCompactionJobs := g.generatePartitionCompactionJobs(blocks, allPartitionedGroup, g.doRandomPick)
	for _, p := range partitionCompactionJobs {
		blockIDs = p.getBlockIDs()
		level.Info(g.logger).Log("msg", "partitioned compaction job", "partitioned_group_id", p.partitionedGroupInfo.PartitionedGroupID, "metric_name_partition_count", p.partitionedGroupInfo.MetricNamePartitionCount, "metric_partition_id", p.metricNamePartitionID, "partition_count", p.partitionedGroupInfo.MetricNamePartitions[p.metricNamePartitionID].PartitionCount, "partition_id", p.partition.PartitionID, "rangeStart", p.rangeStartTime().String(), "rangeEnd", p.rangeEndTime().String(), "blocks", strings.Join(blockIDs, ","))
	}
	return partitionCompactionJobs, nil
}

func (g *PartitionCompactionGrouper) loadExistingPartitionedGroups() (map[uint32]*PartitionedGroupInfo, error) {
	partitionedGroups := make(map[uint32]*PartitionedGroupInfo)
	err := g.bkt.Iter(g.ctx, PartitionedGroupDirectory, func(file string) error {
		if !strings.Contains(file, PartitionVisitMarkerDirectory) {
			partitionedGroup, _, err := ReadPartitionedGroupInfoFile(g.ctx, g.bkt, g.logger, file)
			if err != nil {
				return err
			}
			partitionedGroups[partitionedGroup.PartitionedGroupID] = partitionedGroup
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to load existing partitioned groups")
	}
	return partitionedGroups, nil
}

func (g *PartitionCompactionGrouper) groupBlocks(blocks map[ulid.ULID]*metadata.Meta, timeRanges []int64) []blocksGroupWithPartition {
	// First of all we have to group blocks using the Thanos default
	// grouping (based on downsample resolution + external labels).
	mainGroups := map[string][]*metadata.Meta{}
	for _, b := range blocks {
		key := b.Thanos.GroupKey()
		mainGroups[key] = append(mainGroups[key], b)
	}

	var groups []blocksGroupWithPartition
	for _, mainBlocks := range mainGroups {
		groups = append(groups, g.groupBlocksByCompactableRanges(mainBlocks, timeRanges)...)
	}

	g.sortBlockGroups(groups)

	return groups
}

func (g *PartitionCompactionGrouper) groupBlocksByCompactableRanges(blocks []*metadata.Meta, timeRanges []int64) []blocksGroupWithPartition {
	if len(blocks) == 0 {
		return nil
	}

	// Sort blocks by min time.
	sortMetasByMinTime(blocks)

	var groups []blocksGroupWithPartition

	for _, tr := range timeRanges {
		groups = append(groups, g.groupBlocksByRange(blocks, tr)...)
	}

	return groups
}

func (g *PartitionCompactionGrouper) groupBlocksByRange(blocks []*metadata.Meta, tr int64) []blocksGroupWithPartition {
	var ret []blocksGroupWithPartition

	for i := 0; i < len(blocks); {
		var (
			group blocksGroupWithPartition
			m     = blocks[i]
		)

		group.rangeStart = getRangeStart(m, tr)
		group.rangeEnd = group.rangeStart + tr

		// Skip blocks that don't fall into the range. This can happen via mis-alignment or
		// by being the multiple of the intended range.
		if m.MaxTime > group.rangeEnd {
			i++
			continue
		}

		// Add all blocks to the current group that are within [t0, t0+tr].
		for ; i < len(blocks); i++ {
			// If the block does not start within this group, then we should break the iteration
			// and move it to the next group.
			if blocks[i].MinTime >= group.rangeEnd {
				break
			}

			// If the block doesn't fall into this group, but it started within this group then it
			// means it spans across multiple ranges and we should skip it.
			if blocks[i].MaxTime > group.rangeEnd {
				continue
			}

			group.blocks = append(group.blocks, blocks[i])
		}

		if len(group.blocks) > 1 {
			ret = append(ret, group)
		}
	}

	return ret
}

func (g *PartitionCompactionGrouper) sortBlockGroups(groups []blocksGroupWithPartition) {
	// Ensure groups are sorted by smallest range, oldest min time first. The rationale
	// is that we wanna favor smaller ranges first (ie. to deduplicate samples sooner
	// than later) and older ones are more likely to be "complete" (no missing block still
	// to be uploaded).
	sort.SliceStable(groups, func(i, j int) bool {
		iGroup := groups[i]
		jGroup := groups[j]
		iRangeStart := iGroup.rangeStart
		iRangeEnd := iGroup.rangeEnd
		jRangeStart := jGroup.rangeStart
		jRangeEnd := jGroup.rangeEnd
		iLength := iRangeEnd - iRangeStart
		jLength := jRangeEnd - jRangeStart

		if iLength != jLength {
			return iLength < jLength
		}
		if iRangeStart != jRangeStart {
			return iRangeStart < jRangeStart
		}

		iGroupHash := hashGroup(g.userID, iRangeStart, iRangeEnd)
		iGroupKey := createGroupKeyWithPartition(iGroupHash, iGroup)
		jGroupHash := hashGroup(g.userID, jRangeStart, jRangeEnd)
		jGroupKey := createGroupKeyWithPartition(jGroupHash, jGroup)
		// Guarantee stable sort for tests.
		return iGroupKey < jGroupKey
	})
}

func (g *PartitionCompactionGrouper) generatePartitionedGroups(blocks map[ulid.ULID]*metadata.Meta, groups []blocksGroupWithPartition, existingPartitionedGroups map[uint32]*PartitionedGroupInfo, timeRanges []int64) ([]*PartitionedGroupInfo, error) {
	var allPartitionedGroup []*PartitionedGroupInfo
	for _, partitionedGroup := range existingPartitionedGroups {
		status := partitionedGroup.getPartitionedGroupStatus(g.ctx, g.bkt, g.partitionVisitMarkerTimeout, g.logger)
		if !status.IsCompleted {
			allPartitionedGroup = append(allPartitionedGroup, partitionedGroup)
		}
	}

	timeRangeChecker := NewCompletenessChecker(blocks, groups, timeRanges)
	for _, startTimeMap := range timeRangeChecker.TimeRangesStatus {
		for _, status := range startTimeMap {
			if !status.canTakeCompaction {
				level.Info(g.logger).Log("msg", "incomplete time range", "rangeStart", status.rangeStartTime().String(), "rangeEnd", status.rangeEndTime().String(),
					"timeRange", status.timeRangeDuration().String(), "previousTimeRange", status.previousTimeRangeDuration().String())
			}
		}
	}

	var blockIDs []string
	for _, group := range groups {
		groupHash := hashGroup(g.userID, group.rangeStart, group.rangeEnd)
		logger := log.With(g.logger, "partitioned_group_id", groupHash, "rangeStart", group.rangeStartTime().String(), "rangeEnd", group.rangeEndTime().String())

		blockIDs = group.getBlockIDs()
		level.Info(logger).Log("msg", "block group", "blocks", strings.Join(blockIDs, ","))

		level.Info(logger).Log("msg", "start generating partitioned group")
		if g.shouldSkipGroup(logger, group, groupHash, existingPartitionedGroups, timeRangeChecker) {
			level.Info(logger).Log("msg", "skip generating partitioned group")
			continue
		}
		partitionedGroup, err := g.generatePartitionBlockGroup(group, groupHash)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to generate partitioned group: %d", groupHash)
		}
		level.Info(logger).Log("msg", "generated partitioned group")
		allPartitionedGroup = append(allPartitionedGroup, partitionedGroup)
	}
	return allPartitionedGroup, nil
}

func (g *PartitionCompactionGrouper) shouldSkipGroup(logger log.Logger, group blocksGroupWithPartition, partitionedGroupID uint32, existingPartitionedGroups map[uint32]*PartitionedGroupInfo, timeRangeChecker TimeRangeChecker) bool {
	if _, ok := existingPartitionedGroups[partitionedGroupID]; ok {
		level.Info(logger).Log("msg", "skip group", "reason", "partitioned group already exists")
		return true
	}
	tr := group.rangeEnd - group.rangeStart
	if status, ok := timeRangeChecker.TimeRangesStatus[tr][group.rangeStart]; !ok {
		level.Info(logger).Log("msg", "skip group", "reason", "unable to get time range status")
		return true
	} else if !status.canTakeCompaction {
		level.Info(logger).Log("msg", "skip group", "reason", "time range cannot take compaction job")
		return true
	}

	lastVersion := -1
	// Check if all blocks in group having same partitioned group id as destination partitionedGroupID
	for _, b := range group.blocks {
		metaExtensions, err := tsdb.GetCortexMetaExtensionsFromMeta(*b)
		if err != nil || metaExtensions == nil || metaExtensions.PartitionInfo == nil || metaExtensions.PartitionInfo.PartitionedGroupID != partitionedGroupID {
			return false
		}
		// If group blocks have mixed versions, don't skip group.
		if lastVersion == -1 {
			lastVersion = metaExtensions.Version
		} else if lastVersion != metaExtensions.Version {
			return false
		}
	}
	level.Info(logger).Log("msg", "skip group", "reason", "all blocks in the group have partitioned group id equals to new group partitioned_group_id")
	return true
}

func (g *PartitionCompactionGrouper) generatePartitionBlockGroup(group blocksGroupWithPartition, groupHash uint32) (*PartitionedGroupInfo, error) {
	partitionedGroupInfo, err := g.partitionBlockGroup(group, groupHash)
	if err != nil {
		return nil, err
	}
	updatedPartitionedGroupInfo, err := UpdatePartitionedGroupInfo(g.ctx, g.bkt, g.logger, *partitionedGroupInfo)
	if err != nil {
		return nil, err
	}
	return updatedPartitionedGroupInfo, nil
}

func (g *PartitionCompactionGrouper) partitionBlockGroup(group blocksGroupWithPartition, groupHash uint32) (*PartitionedGroupInfo, error) {
	metricNamePartitionCount := g.calculateMetricNamePartitionCount(group, groupHash)
	partitionedGroups, err := g.partitionBlocksGroup(metricNamePartitionCount, group, groupHash)
	if err != nil {
		return nil, err
	}

	metricNamePartitions := make([]MetricNamePartition, len(partitionedGroups))
	for i, metricNamePartitionedGroup := range partitionedGroups {
		metricNamePartitions[i] = MetricNamePartition{
			MetricNamePartitionID: i,
			PartitionCount:        len(metricNamePartitionedGroup),
		}
		metricNamePartitions[i].Partitions = make([]Partition, metricNamePartitions[i].PartitionCount)
		for j, blockIDs := range metricNamePartitionedGroup {
			metricNamePartitions[i].Partitions[j] = Partition{
				PartitionID: j,
				Blocks:      blockIDs,
			}
		}
	}

	partitionedGroupInfo := PartitionedGroupInfo{
		PartitionedGroupID:       groupHash,
		MetricNamePartitionCount: metricNamePartitionCount,
		MetricNamePartitions:     metricNamePartitions,
		RangeStart:               group.rangeStart,
		RangeEnd:                 group.rangeEnd,
		Version:                  PartitionedGroupInfoVersion2,
	}
	return &partitionedGroupInfo, nil
}

func (g *PartitionCompactionGrouper) calculatePartitionCount(group blocksGroupWithPartition, groupHash uint32, metricNamePartitionCount, metricNamePartitionID int) int {
	indexSizeLimit := g.limits.CompactorPartitionIndexSizeBytes(g.userID)
	oldIndexSizeLimit := g.limits.CompactorPartitionIndexSizeLimitInBytes(g.userID)
	if indexSizeLimit == 0 || oldIndexSizeLimit > 0 && indexSizeLimit > oldIndexSizeLimit {
		indexSizeLimit = oldIndexSizeLimit
	}
	seriesCountLimit := g.limits.CompactorPartitionSeriesCount(g.userID)
	oldSeriesCountLimit := g.limits.CompactorPartitionSeriesCountLimit(g.userID)
	if seriesCountLimit == 0 || oldSeriesCountLimit > 0 && seriesCountLimit > oldSeriesCountLimit {
		seriesCountLimit = oldSeriesCountLimit
	}
	metricNamePartitionSeriesCountLimit := g.limits.CompactorMetricNamePartitionSeriesCountLimit(g.userID)
	smallestRange := g.compactorCfg.BlockRanges.ToMilliseconds()[0]
	groupRange := group.rangeLength()
	if smallestRange >= groupRange {
		// Level 1 to Level 2 compaction, ignore all labels partition if metric name partition is enabled.
		if metricNamePartitionSeriesCountLimit > 0 {
			level.Info(g.logger).Log("msg", "metric name partition enabled at level 1 to level 2 block compaction, set partition count to 1", "partitioned_group_id", groupHash, "smallestRange", smallestRange, "groupRange", groupRange, "metric_name_series_count_limit", metricNamePartitionSeriesCountLimit)
			return 1
		}
		level.Info(g.logger).Log("msg", "calculate level 1 block limits", "partitioned_group_id", groupHash, "smallest_range", smallestRange, "group_range", groupRange, "ingestion_replication_factor", g.ingestionReplicationFactor)
		indexSizeLimit = indexSizeLimit * int64(g.ingestionReplicationFactor)
		seriesCountLimit = seriesCountLimit * int64(g.ingestionReplicationFactor)
	}

	totalIndexSizeInBytes := int64(0)
	totalSeriesCount := int64(0)
	for _, block := range group.blocks {
		blockFiles := block.Thanos.Files
		totalSeriesCount += int64(block.Stats.NumSeries)
		var indexFile *metadata.File
		for _, file := range blockFiles {
			if file.RelPath == thanosblock.IndexFilename {
				indexFile = &file
			}
		}
		if indexFile == nil {
			level.Debug(g.logger).Log("msg", "unable to find index file in metadata", "block", block.ULID)
			break
		}
		indexSize := indexFile.SizeBytes
		totalIndexSizeInBytes += indexSize
	}
	partitionNumberBasedOnIndex := 1
	if indexSizeLimit > 0 && totalIndexSizeInBytes > indexSizeLimit {
		partitionNumberBasedOnIndex = g.findNearestPartitionNumber(float64(totalIndexSizeInBytes), float64(indexSizeLimit))
	}
	partitionNumberBasedOnSeries := 1
	if seriesCountLimit > 0 && totalSeriesCount > seriesCountLimit {
		partitionNumberBasedOnSeries = g.findNearestPartitionNumber(float64(totalSeriesCount), float64(seriesCountLimit))
	}
	partitionNumber := partitionNumberBasedOnIndex
	if partitionNumberBasedOnSeries > partitionNumberBasedOnIndex {
		partitionNumber = partitionNumberBasedOnSeries
	}
	level.Info(g.logger).Log("msg", "calculated partition number for group", "partitioned_group_id", groupHash, "partition_number", partitionNumber, "metric_name_partition_count", metricNamePartitionCount, "metric_name_partition_id", metricNamePartitionID, "total_index_size", totalIndexSizeInBytes, "index_size_limit", indexSizeLimit, "total_series_count", totalSeriesCount, "series_count_limit", seriesCountLimit, "group", group.String())
	return partitionNumber
}

func (g *PartitionCompactionGrouper) calculateMetricNamePartitionCount(group blocksGroupWithPartition, groupHash uint32) int {
	smallestRange := g.compactorCfg.BlockRanges.ToMilliseconds()[0]
	groupRange := group.rangeLength()
	partitionNumber := 1
	seriesCountLimit := g.limits.CompactorMetricNamePartitionSeriesCountLimit(g.userID)
	var totalSeriesCount int64
	if seriesCountLimit > 0 {
		if smallestRange < groupRange {
			for _, block := range group.blocks {
				block := block
				partitionInfo, err := tsdb.GetPartitionInfo(*block)
				if err != nil {
					continue
				}
				if partitionInfo != nil {
					// Find the largest metric name partition count from the block group if exists.
					if partitionInfo.MetricNamePartitionCount > partitionNumber {
						partitionNumber = partitionInfo.MetricNamePartitionCount
					}
				}
			}
		} else {
			// Level 1 to Level 2 compaction.
			for _, block := range group.blocks {
				totalSeriesCount += int64(block.Stats.NumSeries)
			}
			if totalSeriesCount > seriesCountLimit {
				partitionNumber = g.findNearestPartitionNumber(float64(totalSeriesCount), float64(seriesCountLimit))
			}
		}
	}
	level.Info(g.logger).Log("msg", "calculated metric name partition number for group", "partitioned_group_id", groupHash, "smallestRange", smallestRange, "groupRange", groupRange, "partition_number", partitionNumber, "total_series_count", totalSeriesCount, "metric_name_series_count_limit", seriesCountLimit, "group", group.String())
	return partitionNumber
}

func (g *PartitionCompactionGrouper) findNearestPartitionNumber(size float64, limit float64) int {
	return int(math.Pow(2, math.Ceil(math.Log2(size/limit))))
}

func (g *PartitionCompactionGrouper) groupBlocksByMinTime(group blocksGroupWithPartition) map[int64][]*metadata.Meta {
	blocksByMinTime := make(map[int64][]*metadata.Meta)
	for _, block := range group.blocks {
		blockRange := block.MaxTime - block.MinTime
		minTime := block.MinTime
		for _, tr := range g.compactorCfg.BlockRanges.ToMilliseconds() {
			if blockRange <= tr {
				minTime = tr * (block.MinTime / tr)
				break
			}
		}
		blocksByMinTime[minTime] = append(blocksByMinTime[minTime], block)
	}
	return blocksByMinTime
}

func (g *PartitionCompactionGrouper) partitionBlocksGroup(metricNamePartitionCount int, group blocksGroupWithPartition, groupHash uint32) ([][][]ulid.ULID, error) {
	blocksByMinTime := g.groupBlocksByMinTime(group)
	metricNamePartitionedGroups := make([][]*metadata.Meta, metricNamePartitionCount)
	partitionedGroups := make([][][]ulid.ULID, metricNamePartitionCount)
	addToPartitionedGroups := func(block ulid.ULID, metricNamePartitionID, partitionCount, partitionID int) {
		if len(partitionedGroups[metricNamePartitionID]) == 0 {
			partitionedGroups[metricNamePartitionID] = make([][]ulid.ULID, partitionCount)
		}
		if len(partitionedGroups[metricNamePartitionID][partitionID]) == 0 {
			partitionedGroups[metricNamePartitionID][partitionID] = make([]ulid.ULID, 0)
		}

		partitionedGroups[metricNamePartitionID][partitionID] = append(partitionedGroups[metricNamePartitionID][partitionID], block)
	}

	getMetricNamePartitionIDs := func(partitionInfo *tsdb.PartitionInfo) []int {
		output := make([]int, 0)
		if partitionInfo.MetricNamePartitionCount < metricNamePartitionCount {
			for partitionID := partitionInfo.MetricNamePartitionID; partitionID < metricNamePartitionCount; partitionID += partitionInfo.MetricNamePartitionCount {
				output = append(output, partitionID)
			}
		} else if partitionInfo.MetricNamePartitionCount == metricNamePartitionCount {
			output = append(output, partitionInfo.MetricNamePartitionID)
		} else {
			output = append(output, partitionInfo.MetricNamePartitionID%metricNamePartitionCount)
		}
		return output
	}

	getPartitionIDs := func(partitionInfo *tsdb.PartitionInfo, partitionCount int) []int {
		output := make([]int, 0)
		if partitionInfo.PartitionCount < partitionCount {
			for partitionID := partitionInfo.PartitionID; partitionID < partitionCount; partitionID += partitionInfo.PartitionCount {
				output = append(output, partitionID)
			}
		} else if partitionInfo.PartitionCount == partitionCount {
			output = append(output, partitionInfo.PartitionID)
		} else {
			output = append(output, partitionInfo.PartitionID%partitionCount)
		}
		return output
	}

	for _, blocksInSameTimeInterval := range blocksByMinTime {
		for _, block := range blocksInSameTimeInterval {
			block := block
			partitionInfo, err := tsdb.GetPartitionInfo(*block)
			if err != nil {
				return nil, err
			}
			if partitionInfo == nil {
				// For legacy blocks with level > 1, treat PartitionID is always 0.
				// So it can be included in every partition.
				defaultPartitionInfo := tsdb.DefaultPartitionInfo
				partitionInfo = &defaultPartitionInfo
			} else {
				if partitionInfo.MetricNamePartitionCount < 1 {
					partitionInfo.MetricNamePartitionCount = 1
					partitionInfo.MetricNamePartitionID = 0
				}
				if partitionInfo.PartitionCount < 1 {
					partitionInfo.PartitionCount = 1
					partitionInfo.PartitionID = 0
				}
			}
			block.Thanos.Extensions = tsdb.CortexMetaExtensions{
				PartitionInfo: partitionInfo,
				Version:       tsdb.CortexMetaExtensionsVersion1,
			}
			for _, i := range getMetricNamePartitionIDs(partitionInfo) {
				metricNamePartitionedGroups[i] = append(metricNamePartitionedGroups[i], block)
			}
		}
	}

	for i, metricNamePG := range metricNamePartitionedGroups {
		bg := blocksGroupWithPartition{
			blocksGroup: blocksGroup{
				rangeStart: group.rangeStart,
				rangeEnd:   group.rangeEnd,
				blocks:     metricNamePG,
			},
		}
		partitionCount := g.calculatePartitionCount(bg, groupHash, metricNamePartitionCount, i)
		for _, block := range metricNamePG {
			// We already did it above.
			partitionInfo, _ := tsdb.GetPartitionInfo(*block)
			for _, partitionID := range getPartitionIDs(partitionInfo, partitionCount) {
				addToPartitionedGroups(block.ULID, i, partitionCount, partitionID)
			}
		}
	}
	return partitionedGroups, nil
}

func (g *PartitionCompactionGrouper) sortPartitionedGroups(partitionedGroups []*PartitionedGroupInfo) {
	// Ensure groups are sorted by smallest range, oldest min time first. The rationale
	// is that we wanna favor smaller ranges first (ie. to deduplicate samples sooner
	// than later) and older ones are more likely to be "complete" (no missing block still
	// to be uploaded).
	sort.SliceStable(partitionedGroups, func(i, j int) bool {
		iGroup := partitionedGroups[i]
		jGroup := partitionedGroups[j]
		iRangeStart := iGroup.RangeStart
		iRangeEnd := iGroup.RangeEnd
		jRangeStart := jGroup.RangeStart
		jRangeEnd := jGroup.RangeEnd
		iLength := iRangeEnd - iRangeStart
		jLength := jRangeEnd - jRangeStart

		if iLength != jLength {
			return iLength < jLength
		}
		if iRangeStart != jRangeStart {
			return iRangeStart < jRangeStart
		}
		// Guarantee stable sort for tests.
		return iGroup.PartitionedGroupID < jGroup.PartitionedGroupID
	})
}

type partitionIDPair struct {
	metricNamePartitionID int
	partitionID           int
}

func (g *PartitionCompactionGrouper) generatePartitionCompactionJobs(blocks map[ulid.ULID]*metadata.Meta, partitionedGroups []*PartitionedGroupInfo, doRandomPick bool) []*blocksGroupWithPartition {
	var partitionedBlockGroups []*blocksGroupWithPartition
	for _, partitionedGroupInfo := range partitionedGroups {
		partitionedGroupID := partitionedGroupInfo.PartitionedGroupID
		partitionAdded := 0
		var partitionIDs []partitionIDPair
		for _, metricNamePartition := range partitionedGroupInfo.MetricNamePartitions {
			for _, partition := range metricNamePartition.Partitions {
				partitionIDs = append(partitionIDs, partitionIDPair{
					metricNamePartitionID: metricNamePartition.MetricNamePartitionID,
					partitionID:           partition.PartitionID,
				})
			}
		}
		if doRandomPick {
			// Randomly pick partitions from partitioned group to avoid all compactors
			// trying to get same partition at same time.
			r := rand.New(rand.NewSource(time.Now().UnixMicro() + int64(hashString(g.ringLifecyclerID))))
			partitionIdx := r.Perm(len(partitionIDs))
			partitionIDsNew := make([]partitionIDPair, len(partitionIDs))
			for idx, i := range partitionIdx {
				partitionIDsNew[idx] = partitionIDs[i]
			}
			partitionIDs = partitionIDsNew
		}
		for _, idPair := range partitionIDs {
			i := idPair.metricNamePartitionID
			j := idPair.partitionID
			metricNamePartitionCount := partitionedGroupInfo.MetricNamePartitionCount
			metricNamePartitionID := partitionedGroupInfo.MetricNamePartitions[i].MetricNamePartitionID
			partitionCount := partitionedGroupInfo.MetricNamePartitions[i].PartitionCount
			partition := partitionedGroupInfo.MetricNamePartitions[i].Partitions[j]
			if len(partition.Blocks) == 1 {
				partition.Blocks = append(partition.Blocks, DUMMY_BLOCK_ID)
				level.Info(g.logger).Log("msg", "handled single block in partition", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "metric_name_partition_count", metricNamePartitionCount, "metric_name_partition_id", metricNamePartitionID, "partition_count", partitionCount, "partition_id", partition.PartitionID)
			} else if len(partition.Blocks) < 1 {
				if err := g.handleEmptyPartition(partitionedGroupInfo, partition, metricNamePartitionID); err != nil {
					level.Warn(g.logger).Log("msg", "failed to handle empty partition", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "metric_name_partition_count", metricNamePartitionCount, "metric_name_partition_id", metricNamePartitionID, "partition_count", partitionCount, "partition_id", partition.PartitionID)
				}
				continue
			}
			partitionedGroup, err := createBlocksGroup(blocks, partition.Blocks, partitionedGroupInfo.RangeStart, partitionedGroupInfo.RangeEnd)
			if err != nil {
				continue
			}
			partitionedGroup.groupHash = partitionedGroupID
			partitionedGroup.partitionedGroupInfo = partitionedGroupInfo
			partitionedGroup.partition = partition
			partitionedGroup.metricNamePartitionID = i
			partitionedBlockGroups = append(partitionedBlockGroups, partitionedGroup)
			partitionAdded++
		}
	}
	return partitionedBlockGroups
}

// handleEmptyPartition uploads a completed partition visit marker for any partition that does have any blocks assigned
func (g *PartitionCompactionGrouper) handleEmptyPartition(partitionedGroupInfo *PartitionedGroupInfo, partition Partition, metricNamePartitionID int) error {
	if len(partition.Blocks) > 0 {
		return nil
	}

	level.Info(g.logger).Log("msg", "handling empty block partition", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "metric_name_partition_count", partitionedGroupInfo.MetricNamePartitionCount, "metric_name_partition_id", metricNamePartitionID, "partition_count", partitionedGroupInfo.MetricNamePartitions[metricNamePartitionID].PartitionCount, "partition_id", partition.PartitionID)
	visitMarker := &PartitionVisitMarkerWithMetricNamePartition{
		PartitionedGroupID:    partitionedGroupInfo.PartitionedGroupID,
		PartitionID:           partition.PartitionID,
		MetricNamePartitionID: metricNamePartitionID,
	}
	visitMarkerManager := NewVisitMarkerManager(g.bkt, g.logger, g.ringLifecyclerID, visitMarker)
	visitMarkerManager.MarkWithStatus(g.ctx, Completed)

	level.Info(g.logger).Log("msg", "handled empty block in partition", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "metric_name_partition_count", partitionedGroupInfo.MetricNamePartitionCount, "metric_name_partition_id", partitionedGroupInfo.MetricNamePartitions[metricNamePartitionID].MetricNamePartitionID, "partition_count", partitionedGroupInfo.MetricNamePartitions[metricNamePartitionID].PartitionCount, "partition_id", partition.PartitionID)
	return nil
}

func (g *PartitionCompactionGrouper) pickPartitionCompactionJob(partitionCompactionJobs []*blocksGroupWithPartition) []*compact.Group {
	var outGroups []*compact.Group
	timeRangeSet := make(map[int64]struct{})
	for _, partitionedGroup := range partitionCompactionJobs {
		groupHash := partitionedGroup.groupHash
		partitionedGroupID := partitionedGroup.partitionedGroupInfo.PartitionedGroupID
		metricNamePartitionCount := partitionedGroup.partitionedGroupInfo.MetricNamePartitionCount
		metricNamePartitionID := partitionedGroup.metricNamePartitionID
		partitionCount := partitionedGroup.partitionedGroupInfo.MetricNamePartitions[metricNamePartitionID].PartitionCount
		partitionID := partitionedGroup.partition.PartitionID
		partitionedGroupLogger := log.With(g.logger, "rangeStart", partitionedGroup.rangeStartTime().String(), "rangeEnd", partitionedGroup.rangeEndTime().String(), "rangeDuration", partitionedGroup.rangeDuration().String(), "partitioned_group_id", partitionedGroupID, "metric_name_partition_id", metricNamePartitionID, "metric_name_partition_count", metricNamePartitionCount, "partition_id", partitionID, "partition_count", partitionCount, "group_hash", groupHash)
		visitMarker := NewPartitionVisitMarkerWithMetricNamePartition(g.ringLifecyclerID, partitionedGroupID, metricNamePartitionID, partitionID)
		visitMarkerManager := NewVisitMarkerManager(g.bkt, g.logger, g.ringLifecyclerID, visitMarker)
		if isVisited, err := g.isGroupVisited(metricNamePartitionID, partitionID, visitMarkerManager); err != nil {
			level.Warn(partitionedGroupLogger).Log("msg", "unable to check if partition is visited", "err", err, "group", partitionedGroup.String())
			continue
		} else if isVisited {
			level.Info(partitionedGroupLogger).Log("msg", "skipping group because partition is visited")
			continue
		}
		partitionedGroupKey := createGroupKeyWithMetricNameAndPartitionID(groupHash, metricNamePartitionID, partitionID, *partitionedGroup)

		level.Info(partitionedGroupLogger).Log("msg", "found compactable group for user", "group", partitionedGroup.String())
		begin := time.Now()

		visitMarkerManager.MarkWithStatus(g.ctx, Pending)
		level.Info(partitionedGroupLogger).Log("msg", "marked partition visited in group", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds(), "group", partitionedGroup.String())

		resolution := partitionedGroup.blocks[0].Thanos.Downsample.Resolution
		externalLabels := labels.FromMap(partitionedGroup.blocks[0].Thanos.Labels)
		timeRange := partitionedGroup.rangeEnd - partitionedGroup.rangeStart
		metricLabelValues := []string{
			g.userID,
			fmt.Sprintf("%d", timeRange),
		}
		// Only initialize and expose metrics once per time range.
		if _, ok := timeRangeSet[timeRange]; !ok {
			g.compactorMetrics.initMetricWithCompactionLabelValues(metricLabelValues...)
			var (
				totalPartitions   int
				maxPartitionCount int
			)
			for _, metricNamePartition := range partitionedGroup.partitionedGroupInfo.MetricNamePartitions {
				totalPartitions += metricNamePartition.PartitionCount
				if metricNamePartition.PartitionCount > maxPartitionCount {
					maxPartitionCount = metricNamePartition.PartitionCount
				}
			}
			g.compactorMetrics.metricNamePartitionCount.WithLabelValues(metricLabelValues...).Set(float64(partitionedGroup.partitionedGroupInfo.MetricNamePartitionCount))
			g.compactorMetrics.totalPartitionCount.WithLabelValues(metricLabelValues...).Set(float64(totalPartitions))
			g.compactorMetrics.maxPartitionsPerMetricNamePartition.WithLabelValues(metricLabelValues...).Set(float64(maxPartitionCount))
			timeRangeSet[timeRange] = struct{}{}
		}
		thanosGroup, err := compact.NewGroup(
			log.With(partitionedGroupLogger, "groupKey", partitionedGroupKey, "externalLabels", externalLabels, "downsampleResolution", resolution),
			g.bkt,
			partitionedGroupKey,
			externalLabels,
			resolution,
			g.acceptMalformedIndex,
			true, // Enable vertical compaction.
			g.compactorMetrics.compactions.WithLabelValues(metricLabelValues...),
			g.compactorMetrics.compactionRunsStarted.WithLabelValues(metricLabelValues...),
			g.compactorMetrics.compactionRunsCompleted.WithLabelValues(metricLabelValues...),
			g.compactorMetrics.compactionFailures.WithLabelValues(metricLabelValues...),
			g.compactorMetrics.verticalCompactions.WithLabelValues(metricLabelValues...),
			g.syncerMetrics.GarbageCollectedBlocks,
			g.syncerMetrics.BlocksMarkedForDeletion,
			g.blocksMarkedForNoCompact,
			g.hashFunc,
			g.blockFilesConcurrency,
			g.blocksFetchConcurrency,
		)
		if err != nil {
			level.Error(partitionedGroupLogger).Log("msg", "failed to create partitioned group", "blocks", partitionedGroup.partition.Blocks)
		}

		blockPartitionInfos := make(map[ulid.ULID]tsdb.BlockPartitionInfo, len(partitionedGroup.blocks))
		for _, m := range partitionedGroup.blocks {
			if err := thanosGroup.AppendMeta(m); err != nil {
				level.Error(partitionedGroupLogger).Log("msg", "failed to add block to partitioned group", "block", m.ULID, "err", err)
			}
			partitionInfo, err := tsdb.GetPartitionInfo(*m)
			if err != nil {
				// This shouldn't happen as each block's partition info should have been validated before.
				// But in case this happens, skip setting block partition info.
				continue
			}
			if partitionInfo.PartitionCount == 0 {
				partitionInfo.PartitionCount = 1
			}
			if partitionInfo.MetricNamePartitionCount == 0 {
				partitionInfo.MetricNamePartitionCount = 1
			}
			blockPartitionInfos[m.ULID] = tsdb.BlockPartitionInfo{
				PartitionCount:           partitionInfo.PartitionCount,
				MetricNamePartitionCount: partitionInfo.MetricNamePartitionCount,
			}
		}
		thanosGroup.SetExtensions(&tsdb.CortexMetaExtensions{
			PartitionInfo: &tsdb.PartitionInfo{
				PartitionedGroupID:           partitionedGroupID,
				MetricNamePartitionCount:     metricNamePartitionCount,
				MetricNamePartitionID:        metricNamePartitionID,
				PartitionCount:               partitionCount,
				PartitionID:                  partitionID,
				PartitionedGroupCreationTime: partitionedGroup.partitionedGroupInfo.CreationTime,
				BlockPartitionInfos:          blockPartitionInfos,
			},
			TimeRange: timeRange,
			Version:   tsdb.CortexMetaExtensionsVersion1,
		})

		outGroups = append(outGroups, thanosGroup)
		level.Debug(partitionedGroupLogger).Log("msg", "added partition to compaction groups")
		if len(outGroups) >= g.compactionConcurrency {
			break
		}
	}

	level.Info(g.logger).Log("msg", fmt.Sprintf("total groups for compaction: %d", len(outGroups)))

	for _, p := range outGroups {
		partitionInfo, err := tsdb.ConvertToPartitionInfo(p.Extensions())
		if err == nil && partitionInfo != nil {
			level.Info(g.logger).Log("msg", "picked compaction job", "partitioned_group_id", partitionInfo.PartitionedGroupID, "metric_name_partition_count", partitionInfo.MetricNamePartitionCount, "metric_name_partition_id", partitionInfo.MetricNamePartitionID, "partition_count", partitionInfo.PartitionCount, "partition_id", partitionInfo.PartitionID)
		}
	}
	return outGroups
}

func (g *PartitionCompactionGrouper) isGroupVisited(metricNamePartitionID, partitionID int, visitMarkerManager *VisitMarkerManager) (bool, error) {
	visitMarker := &PartitionVisitMarkerWithMetricNamePartition{}
	err := visitMarkerManager.ReadVisitMarker(g.ctx, visitMarker)
	if err != nil {
		if errors.Is(err, errorVisitMarkerNotFound) {
			level.Warn(g.logger).Log("msg", "no visit marker file for partition", "partition_visit_marker_file", visitMarkerManager.visitMarker.GetVisitMarkerFilePath())
			return false, nil
		}
		level.Error(g.logger).Log("msg", "unable to read partition visit marker file", "partition_visit_marker_file", visitMarkerManager.visitMarker.GetVisitMarkerFilePath(), "err", err)
		return true, err
	}
	if visitMarker.GetStatus() == Completed {
		level.Info(g.logger).Log("msg", "partition visit marker with partition ID is completed", "partition_visit_marker", visitMarker.String())
		return true, nil
	}
	if visitMarker.IsVisited(g.partitionVisitMarkerTimeout, metricNamePartitionID, partitionID) {
		level.Info(g.logger).Log("msg", "visited partition with partition ID", "partition_visit_marker", visitMarker.String())
		return true, nil
	}
	return false, nil
}

type TimeRangeChecker struct {
	// This is a map of timeRange to a map of rangeStart to timeRangeStatus
	TimeRangesStatus map[int64]map[int64]*timeRangeStatus
}

func NewCompletenessChecker(blocks map[ulid.ULID]*metadata.Meta, groups []blocksGroupWithPartition, timeRanges []int64) TimeRangeChecker {
	timeRangeToBlockMap := make(map[int64][]*metadata.Meta)
	for _, b := range blocks {
		timeRange := int64(0)
		if b.Compaction.Level > 1 {
			ext, err := tsdb.GetCortexMetaExtensionsFromMeta(*b)
			if err == nil && ext != nil && ext.TimeRange > 0 {
				timeRange = ext.TimeRange
			} else {
				// fallback logic to guess block time range based
				// on MaxTime and MinTime
				blockRange := b.MaxTime - b.MinTime
				for _, tr := range timeRanges {
					rangeStart := getRangeStart(b, tr)
					rangeEnd := rangeStart + tr
					if tr >= blockRange && rangeEnd >= b.MaxTime {
						timeRange = tr
						break
					}
				}
			}
		}
		timeRangeToBlockMap[timeRange] = append(timeRangeToBlockMap[timeRange], b)
	}
	timeRangesStatus := make(map[int64]map[int64]*timeRangeStatus)
	for _, g := range groups {
		tr := g.rangeEnd - g.rangeStart
		if _, ok := timeRangesStatus[tr]; !ok {
			timeRangesStatus[tr] = make(map[int64]*timeRangeStatus)
		}
		timeRangesStatus[tr][g.rangeStart] = &timeRangeStatus{
			timeRange:         tr,
			rangeStart:        g.rangeStart,
			rangeEnd:          g.rangeEnd,
			numActiveBlocks:   0,
			canTakeCompaction: false,
		}
	}
	for tr, blks := range timeRangeToBlockMap {
		if _, ok := timeRangesStatus[tr]; !ok {
			timeRangesStatus[tr] = make(map[int64]*timeRangeStatus)
		}
		for _, b := range blks {
			actualTr := tr
			if tr == 0 {
				actualTr = timeRanges[0]
			}
			rangeStart := getRangeStart(b, actualTr)
			if _, ok := timeRangesStatus[tr][rangeStart]; !ok {
				timeRangesStatus[tr][rangeStart] = &timeRangeStatus{
					timeRange:         tr,
					rangeStart:        rangeStart,
					rangeEnd:          rangeStart + actualTr,
					numActiveBlocks:   0,
					canTakeCompaction: false,
				}
			}
			timeRangesStatus[tr][rangeStart].addBlock(1)
		}
	}
	previousTimeRanges := []int64{0}
	for _, tr := range timeRanges {
	timeRangeLoop:
		for rangeStart, status := range timeRangesStatus[tr] {
			previousTrBlocks := 0
			for _, previousTr := range previousTimeRanges {
				allPreviousTimeRanges := getAllPreviousTimeRanges(tr, rangeStart, previousTr, timeRanges[0])
				for _, previousRangeStart := range allPreviousTimeRanges {
					if previousTrStatus, ok := timeRangesStatus[previousTr][previousRangeStart]; ok {
						if previousTrStatus.canTakeCompaction {
							status.canTakeCompaction = false
							continue timeRangeLoop
						}
						previousTrBlocks += previousTrStatus.numActiveBlocks
					}
				}
			}
			status.canTakeCompaction = !(previousTrBlocks == 0 || (previousTrBlocks == 1 && status.numActiveBlocks == 0)) //nolint:staticcheck
		}
		previousTimeRanges = append(previousTimeRanges, tr)
	}
	return TimeRangeChecker{TimeRangesStatus: timeRangesStatus}
}

// getAllPreviousTimeRanges returns a list of rangeStart time for previous time range that
// falls within current time range and start time
func getAllPreviousTimeRanges(currentTr int64, rangeStart int64, previousTr int64, smallestTr int64) []int64 {
	var result []int64
	if previousTr == 0 {
		previousTr = smallestTr
	}
	previousRangeStart := rangeStart
	for ; previousRangeStart+previousTr <= rangeStart+currentTr; previousRangeStart += previousTr {
		result = append(result, previousRangeStart)
	}
	return result
}

type timeRangeStatus struct {
	timeRange         int64
	rangeStart        int64
	rangeEnd          int64
	numActiveBlocks   int
	canTakeCompaction bool
	previousTimeRange int64
}

func (t *timeRangeStatus) addBlock(num int) {
	t.numActiveBlocks += num
}

func (t *timeRangeStatus) rangeStartTime() time.Time {
	return time.Unix(0, t.rangeStart*int64(time.Millisecond)).UTC()
}

func (t *timeRangeStatus) rangeEndTime() time.Time {
	return time.Unix(0, t.rangeEnd*int64(time.Millisecond)).UTC()
}

func (t *timeRangeStatus) timeRangeDuration() time.Duration {
	return time.Duration(t.timeRange) * time.Millisecond
}

func (t *timeRangeStatus) previousTimeRangeDuration() time.Duration {
	return time.Duration(t.previousTimeRange) * time.Millisecond
}

type blocksGroupWithPartition struct {
	blocksGroup
	groupHash             uint32
	partitionedGroupInfo  *PartitionedGroupInfo
	partition             Partition
	metricNamePartitionID int
}

func (g blocksGroupWithPartition) rangeDuration() time.Duration {
	return g.rangeEndTime().Sub(g.rangeStartTime())
}

func (g blocksGroupWithPartition) getBlockIDs() []string {
	blockIDs := make([]string, len(g.blocks))
	for i, block := range g.blocks {
		blockIDs[i] = block.ULID.String()
	}
	return blockIDs
}

func createGroupKeyWithPartition(groupHash uint32, group blocksGroupWithPartition) string {
	return fmt.Sprintf("%v%s", groupHash, group.blocks[0].Thanos.GroupKey())
}

func createGroupKeyWithMetricNameAndPartitionID(groupHash uint32, metricNamePartitionID, partitionID int, group blocksGroupWithPartition) string {
	return fmt.Sprintf("%v:%d:%d:%s", groupHash, metricNamePartitionID, partitionID, group.blocks[0].Thanos.GroupKey())
}

func createBlocksGroup(blocks map[ulid.ULID]*metadata.Meta, blockIDs []ulid.ULID, rangeStart int64, rangeEnd int64) (*blocksGroupWithPartition, error) {
	var group blocksGroupWithPartition
	group.rangeStart = rangeStart
	group.rangeEnd = rangeEnd
	var nonDummyBlock *metadata.Meta
	for _, blockID := range blockIDs {
		if blockID == DUMMY_BLOCK_ID {
			continue
		}
		m, ok := blocks[blockID]
		if !ok {
			return nil, fmt.Errorf("block not found: %s", blockID)
		}
		nonDummyBlock = m
		group.blocks = append(group.blocks, m)
	}
	for _, blockID := range blockIDs {
		if blockID == DUMMY_BLOCK_ID {
			dummyMeta := *nonDummyBlock
			dummyMeta.ULID = DUMMY_BLOCK_ID
			group.blocks = append(group.blocks, &dummyMeta)
		}
	}
	return &group, nil
}
