package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/util/runutil"
)

const (
	PartitionedGroupDirectory    = "partitioned-groups"
	PartitionedGroupInfoVersion1 = 1
	PartitionedGroupInfoVersion2 = 2
)

var (
	SupportedPartitionedGroupInfoVersions = []int{
		PartitionedGroupInfoVersion1,
		PartitionedGroupInfoVersion2,
	}
	ErrorPartitionedGroupInfoNotFound  = errors.New("partitioned group info not found")
	ErrorUnmarshalPartitionedGroupInfo = errors.New("unmarshal partitioned group info JSON")
	ErrorMarshalPartitionedGroupInfo   = errors.New("marshal partitioned group info JSON")
)

type Partition struct {
	PartitionID int         `json:"partitionID"`
	Blocks      []ulid.ULID `json:"blocks"`
}

type MetricNamePartition struct {
	PartitionCount        int         `json:"partitionCount"`
	MetricNamePartitionID int         `json:"metricNamePartitionID"`
	Partitions            []Partition `json:"partitions"`
}

type PartitionWithMetricNamePartitionID struct {
	Partition
	MetricNamePartitionID int
}

type PartitionedGroupStatus struct {
	PartitionedGroupID        uint32
	CanDelete                 bool
	IsCompleted               bool
	DeleteVisitMarker         bool
	PendingPartitions         int
	InProgressPartitions      int
	PendingOrFailedPartitions []PartitionWithMetricNamePartitionID
}

func (s PartitionedGroupStatus) String() string {
	var partitions []string
	for _, p := range s.PendingOrFailedPartitions {
		partitions = append(partitions, fmt.Sprintf("%d_%d", p.MetricNamePartitionID, p.PartitionID))
	}
	return fmt.Sprintf(`{"partitioned_group_id": %d, "can_delete": %t, "is_complete": %t, "delete_visit_marker": %t, "pending_partitions": %d, "in_progress_partitions": %d, "pending_or_failed_partitions": [%s]}`,
		s.PartitionedGroupID, s.CanDelete, s.IsCompleted, s.DeleteVisitMarker, s.PendingPartitions, s.InProgressPartitions, strings.Join(partitions, ","))
}

type PartitionedGroupInfo struct {
	PartitionedGroupID       uint32                `json:"partitionedGroupID"`
	MetricNamePartitionCount int                   `json:"metricNamePartitionCount,omitempty"`
	MetricNamePartitions     []MetricNamePartition `json:"metricNamePartitions,omitempty"`
	RangeStart               int64                 `json:"rangeStart"`
	RangeEnd                 int64                 `json:"rangeEnd"`
	CreationTime             int64                 `json:"creationTime"`
	// Version of the file.
	Version int `json:"version"`
}

type PartitionedGroupInfoV1 struct {
	PartitionedGroupID uint32      `json:"partitionedGroupID"`
	PartitionCount     int         `json:"partitionCount"`
	Partitions         []Partition `json:"partitions"`
	RangeStart         int64       `json:"rangeStart"`
	RangeEnd           int64       `json:"rangeEnd"`
	CreationTime       int64       `json:"creationTime"`
	// Version of the file.
	Version int `json:"version"`
}

// ToPartitionedGroupInfo converts PartitionedGroupInfoV1 to latest PartitionedGroupInfo.
func (p *PartitionedGroupInfoV1) ToPartitionedGroupInfo() PartitionedGroupInfo {
	return PartitionedGroupInfo{
		// Even though we convert group info to the new version, keep
		// the group ID as the same since it is used to locate visit markers.
		PartitionedGroupID:       p.PartitionedGroupID,
		MetricNamePartitionCount: 1,
		MetricNamePartitions: []MetricNamePartition{
			{
				MetricNamePartitionID: 0,
				PartitionCount:        p.PartitionCount,
				Partitions:            p.Partitions,
			},
		},
		RangeStart:   p.RangeStart,
		RangeEnd:     p.RangeEnd,
		CreationTime: p.CreationTime,
		Version:      PartitionedGroupInfoVersion2,
	}
}

func (p *PartitionedGroupInfo) rangeStartTime() time.Time {
	return time.Unix(0, p.RangeStart*int64(time.Millisecond)).UTC()
}

func (p *PartitionedGroupInfo) rangeEndTime() time.Time {
	return time.Unix(0, p.RangeEnd*int64(time.Millisecond)).UTC()
}

func (p *PartitionedGroupInfo) getAllBlocks() []ulid.ULID {
	if len(p.MetricNamePartitions) == 0 {
		return nil
	}
	uniqueBlocks := make(map[ulid.ULID]struct{})
	for _, metricNamePartition := range p.MetricNamePartitions {
		for _, partition := range metricNamePartition.Partitions {
			for _, b := range partition.Blocks {
				uniqueBlocks[b] = struct{}{}
			}
		}
	}
	blocks := make([]ulid.ULID, len(uniqueBlocks))
	i := 0
	for b := range uniqueBlocks {
		blocks[i] = b
		i++
	}

	return blocks
}

func (p *PartitionedGroupInfo) getAllBlockIDs() []string {
	blocks := p.getAllBlocks()
	blockIDs := make([]string, len(blocks))
	for i, block := range blocks {
		blockIDs[i] = block.String()
	}
	return blockIDs
}

func (p *PartitionedGroupInfo) getPartitionedGroupStatus(
	ctx context.Context,
	userBucket objstore.InstrumentedBucket,
	partitionVisitMarkerTimeout time.Duration,
	userLogger log.Logger,
) PartitionedGroupStatus {
	status := PartitionedGroupStatus{
		PartitionedGroupID:        p.PartitionedGroupID,
		CanDelete:                 false,
		IsCompleted:               false,
		DeleteVisitMarker:         false,
		PendingPartitions:         0,
		InProgressPartitions:      0,
		PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{},
	}
	allPartitionCompleted := true
	hasInProgressPartitions := false
	for _, metricNamePartition := range p.MetricNamePartitions {
		for _, partition := range metricNamePartition.Partitions {
			visitMarker := &PartitionVisitMarkerWithMetricNamePartition{
				PartitionedGroupID:    p.PartitionedGroupID,
				PartitionID:           partition.PartitionID,
				MetricNamePartitionID: metricNamePartition.MetricNamePartitionID,
			}
			visitMarkerManager := NewVisitMarkerManager(userBucket, userLogger, "PartitionedGroupInfo.getPartitionedGroupStatus", visitMarker)
			partitionVisitMarkerExists := true
			if err := visitMarkerManager.ReadVisitMarker(ctx, visitMarker); err != nil {
				if errors.Is(err, errorVisitMarkerNotFound) {
					partitionVisitMarkerExists = false
				} else {
					level.Warn(userLogger).Log("msg", "unable to read partition visit marker", "path", visitMarker.GetVisitMarkerFilePath(), "err", err)
					return status
				}
			}

			if !partitionVisitMarkerExists {
				status.PendingPartitions++
				allPartitionCompleted = false
				status.PendingOrFailedPartitions = append(status.PendingOrFailedPartitions, PartitionWithMetricNamePartitionID{
					MetricNamePartitionID: metricNamePartition.MetricNamePartitionID,
					Partition:             partition,
				})
			} else if visitMarker.VisitTime < p.CreationTime {
				status.DeleteVisitMarker = true
				allPartitionCompleted = false
			} else if (visitMarker.GetStatus() == Pending || visitMarker.GetStatus() == InProgress) && !visitMarker.IsExpired(partitionVisitMarkerTimeout) {
				status.InProgressPartitions++
				hasInProgressPartitions = true
				allPartitionCompleted = false
			} else if visitMarker.GetStatus() != Completed {
				status.PendingPartitions++
				allPartitionCompleted = false
				status.PendingOrFailedPartitions = append(status.PendingOrFailedPartitions, PartitionWithMetricNamePartitionID{
					MetricNamePartitionID: metricNamePartition.MetricNamePartitionID,
					Partition:             partition,
				})
			}
		}
	}

	if hasInProgressPartitions {
		return status
	}

	status.IsCompleted = allPartitionCompleted

	if allPartitionCompleted {
		status.CanDelete = true
		status.DeleteVisitMarker = true
		return status
	}

	checkedBlocks := make(map[ulid.ULID]struct{})
	for _, partition := range status.PendingOrFailedPartitions {
		for _, blockID := range partition.Blocks {
			if _, ok := checkedBlocks[blockID]; ok {
				continue
			}
			if !p.doesBlockExist(ctx, userBucket, userLogger, blockID) {
				level.Info(userLogger).Log("msg", "delete partitioned group", "reason", "block is physically deleted", "block", blockID)
				status.CanDelete = true
				status.DeleteVisitMarker = true
				return status
			}
			if p.isBlockDeleted(ctx, userBucket, userLogger, blockID) {
				level.Info(userLogger).Log("msg", "delete partitioned group", "reason", "block is marked for deletion", "block", blockID)
				status.CanDelete = true
				status.DeleteVisitMarker = true
				return status
			}
			if p.isBlockNoCompact(ctx, userBucket, userLogger, blockID) {
				level.Info(userLogger).Log("msg", "delete partitioned group", "reason", "block is marked for no compact", "block", blockID)
				status.CanDelete = true
				status.DeleteVisitMarker = true
				return status
			}
			checkedBlocks[blockID] = struct{}{}
		}
	}
	return status
}

func (p *PartitionedGroupInfo) doesBlockExist(ctx context.Context, userBucket objstore.InstrumentedBucket, userLogger log.Logger, blockID ulid.ULID) bool {
	metaExists, err := userBucket.Exists(ctx, path.Join(blockID.String(), metadata.MetaFilename))
	if err != nil {
		level.Warn(userLogger).Log("msg", "unable to get stats of meta.json for block", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
		return true
	}
	return metaExists
}

func (p *PartitionedGroupInfo) isBlockDeleted(ctx context.Context, userBucket objstore.InstrumentedBucket, userLogger log.Logger, blockID ulid.ULID) bool {
	deletionMarkerExists, err := userBucket.Exists(ctx, path.Join(blockID.String(), metadata.DeletionMarkFilename))
	if err != nil {
		level.Warn(userLogger).Log("msg", "unable to get stats of deletion-mark.json for block", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
		return false
	}
	return deletionMarkerExists
}

func (p *PartitionedGroupInfo) isBlockNoCompact(ctx context.Context, userBucket objstore.InstrumentedBucket, userLogger log.Logger, blockID ulid.ULID) bool {
	noCompactMarkerExists, err := userBucket.Exists(ctx, path.Join(blockID.String(), metadata.NoCompactMarkFilename))
	if err != nil {
		level.Warn(userLogger).Log("msg", "unable to get stats of no-compact-mark.json for block", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
		return false
	}
	return noCompactMarkerExists
}

func (p *PartitionedGroupInfo) markAllBlocksForDeletion(ctx context.Context, userBucket objstore.InstrumentedBucket, userLogger log.Logger, blocksMarkedForDeletion *prometheus.CounterVec, userID string) error {
	blocks := p.getAllBlocks()
	deleteBlocksCount := 0
	defer func() {
		level.Info(userLogger).Log("msg", "total number of blocks marked for deletion during partitioned group info clean up", "count", deleteBlocksCount)
	}()
	for _, blockID := range blocks {
		if p.doesBlockExist(ctx, userBucket, userLogger, blockID) && !p.isBlockDeleted(ctx, userBucket, userLogger, blockID) && !p.isBlockNoCompact(ctx, userBucket, userLogger, blockID) {
			if err := block.MarkForDeletion(ctx, userLogger, userBucket, blockID, "delete block during partitioned group completion check", blocksMarkedForDeletion.WithLabelValues(userID, reasonValueRetention)); err != nil {
				level.Warn(userLogger).Log("msg", "unable to mark block for deletion", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
				return err
			}
			deleteBlocksCount++
			level.Debug(userLogger).Log("msg", "marked block for deletion during partitioned group info clean up", "partitioned_group_id", p.PartitionedGroupID, "block", blockID.String())
		}
	}
	return nil
}

func (p *PartitionedGroupInfo) String() string {
	var metricNamePartitions []string
	for _, metricNamePartition := range p.MetricNamePartitions {
		partitions := make([]string, len(metricNamePartition.Partitions))
		for i, partition := range metricNamePartition.Partitions {
			partitions[i] = fmt.Sprintf("(PartitionID: %d, Blocks: %s)", partition.PartitionID, partition.Blocks)
		}
		metricNamePartitions = append(metricNamePartitions, fmt.Sprintf("{MetricNamePartitionID: %d, PartitionCount: %d, Partitions: %s}", metricNamePartition.MetricNamePartitionID, metricNamePartition.PartitionCount, strings.Join(partitions, ", ")))
	}
	return fmt.Sprintf("{PartitionedGroupID: %d, MetricNamePartitionCount: %d, MetricNamePartitions: %s}", p.PartitionedGroupID, p.MetricNamePartitionCount, strings.Join(metricNamePartitions, ", "))
}

func GetPartitionedGroupFile(partitionedGroupID uint32) string {
	return path.Join(PartitionedGroupDirectory, fmt.Sprintf("%d.json", partitionedGroupID))
}

func ReadPartitionedGroupInfo(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, partitionedGroupID uint32) (*PartitionedGroupInfo, bool, error) {
	return ReadPartitionedGroupInfoFile(ctx, bkt, logger, GetPartitionedGroupFile(partitionedGroupID))
}

func ReadPartitionedGroupInfoFile(ctx context.Context, bkt objstore.InstrumentedBucketReader, logger log.Logger, partitionedGroupFile string) (*PartitionedGroupInfo, bool, error) {
	converted := false
	partitionedGroupReader, err := bkt.ReaderWithExpectedErrs(bkt.IsObjNotFoundErr).Get(ctx, partitionedGroupFile)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, false, errors.Wrapf(ErrorPartitionedGroupInfoNotFound, "partitioned group file: %s", partitionedGroupFile)
		}
		return nil, false, errors.Wrapf(err, "get partitioned group file: %s", partitionedGroupFile)
	}
	defer runutil.CloseWithLogOnErr(logger, partitionedGroupReader, "close partitioned group reader")
	p, err := io.ReadAll(partitionedGroupReader)
	if err != nil {
		return nil, false, errors.Wrapf(err, "read partitioned group file: %s", partitionedGroupFile)
	}
	type version struct {
		// Version of the file.
		Version int `json:"version"`
	}
	var v version
	if err = json.Unmarshal(p, &v); err != nil {
		return nil, false, errors.Wrapf(ErrorUnmarshalPartitionedGroupInfo, "partitioned group file: %s, error: %v", partitionedGroupFile, err.Error())
	}
	if !slices.Contains(SupportedPartitionedGroupInfoVersions, v.Version) {
		return nil, false, errors.Errorf("unexpected partitioned group file version %d, expected %v", v.Version, SupportedPartitionedGroupInfoVersions)
	}
	partitionedGroupInfo := PartitionedGroupInfo{}
	switch v.Version {
	case PartitionedGroupInfoVersion1:
		partitionedGroupInfoV1 := PartitionedGroupInfoV1{}
		if err = json.Unmarshal(p, &partitionedGroupInfoV1); err != nil {
			return nil, false, errors.Wrapf(ErrorUnmarshalPartitionedGroupInfo, "partitioned group file: %s, error: %v", partitionedGroupFile, err.Error())
		}
		partitionedGroupInfo = partitionedGroupInfoV1.ToPartitionedGroupInfo()
		converted = true
	case PartitionedGroupInfoVersion2:
		if err = json.Unmarshal(p, &partitionedGroupInfo); err != nil {
			return nil, false, errors.Wrapf(ErrorUnmarshalPartitionedGroupInfo, "partitioned group file: %s, error: %v", partitionedGroupFile, err.Error())
		}
	}

	if partitionedGroupInfo.CreationTime <= 0 {
		objAttr, err := bkt.Attributes(ctx, partitionedGroupFile)
		if err != nil {
			return nil, converted, errors.Errorf("unable to get partitioned group file attributes: %s, error: %v", partitionedGroupFile, err.Error())
		}
		partitionedGroupInfo.CreationTime = objAttr.LastModified.Unix()
	}
	return &partitionedGroupInfo, converted, nil
}

func UpdatePartitionedGroupInfo(ctx context.Context, bkt objstore.InstrumentedBucket, logger log.Logger, partitionedGroupInfo PartitionedGroupInfo) (*PartitionedGroupInfo, error) {
	// Ignore error in order to always update partitioned group info. There is no harm to put latest version of
	// partitioned group info which is supposed to be the correct grouping based on latest bucket store.
	existingPartitionedGroup, converted, _ := ReadPartitionedGroupInfo(ctx, bkt, logger, partitionedGroupInfo.PartitionedGroupID)
	if existingPartitionedGroup != nil && !converted {
		level.Warn(logger).Log("msg", "partitioned group info already exists", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID)
		return existingPartitionedGroup, nil
	}
	// If converted, let's try to upload the new version file again.
	if partitionedGroupInfo.CreationTime <= 0 {
		partitionedGroupInfo.CreationTime = time.Now().Unix()
	}
	partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupInfo.PartitionedGroupID)
	partitionedGroupInfoContent, err := json.Marshal(partitionedGroupInfo)
	if err != nil {
		return nil, errors.Wrapf(ErrorMarshalPartitionedGroupInfo, "partitioned group: %d, error: %v", partitionedGroupInfo.PartitionedGroupID, err.Error())
	}
	reader := bytes.NewReader(partitionedGroupInfoContent)
	if err := bkt.Upload(ctx, partitionedGroupFile, reader); err != nil {
		return nil, errors.Wrapf(err, "unable to upload partitioned group file: %s", partitionedGroupFile)
	}
	level.Info(logger).Log("msg", "created new partitioned group info", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID)
	return &partitionedGroupInfo, nil
}

// UpdatePartitionedGroupInfoV1 is only used in tests.
func UpdatePartitionedGroupInfoV1(ctx context.Context, bkt objstore.InstrumentedBucket, logger log.Logger, partitionedGroupInfo PartitionedGroupInfoV1) (*PartitionedGroupInfoV1, error) {
	if partitionedGroupInfo.CreationTime <= 0 {
		partitionedGroupInfo.CreationTime = time.Now().Unix()
	}
	partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupInfo.PartitionedGroupID)
	partitionedGroupInfoContent, err := json.Marshal(partitionedGroupInfo)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(partitionedGroupInfoContent)
	if err := bkt.Upload(ctx, partitionedGroupFile, reader); err != nil {
		return nil, err
	}
	level.Info(logger).Log("msg", "created new partitioned group info", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID)
	return &partitionedGroupInfo, nil
}
