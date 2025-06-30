package compactor

import (
	"context"
	"encoding/json"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

func TestPartitionedGroupInfoV1(t *testing.T) {
	ulid0 := ulid.MustNew(0, nil)
	ulid1 := ulid.MustNew(1, nil)
	ulid2 := ulid.MustNew(2, nil)
	rangeStart := (1 * time.Hour).Milliseconds()
	rangeEnd := (2 * time.Hour).Milliseconds()
	partitionedGroupID := uint32(12345)
	for _, tcase := range []struct {
		name                 string
		partitionedGroupInfo PartitionedGroupInfoV1
	}{
		{
			name: "write partitioned group info 1",
			partitionedGroupInfo: PartitionedGroupInfoV1{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart: rangeStart,
				RangeEnd:   rangeEnd,
				Version:    PartitionedGroupInfoVersion1,
			},
		},
		{
			name: "write partitioned group info 2",
			partitionedGroupInfo: PartitionedGroupInfoV1{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     3,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid1,
						},
					},
					{
						PartitionID: 2,
						Blocks: []ulid.ULID{
							ulid2,
						},
					},
				},
				RangeStart: rangeStart,
				RangeEnd:   rangeEnd,
				Version:    PartitionedGroupInfoVersion1,
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			ctx := context.Background()
			testBkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
			bkt := objstore.WithNoopInstr(testBkt)
			logger := log.NewNopLogger()
			writeRes, err := UpdatePartitionedGroupInfoV1(ctx, bkt, logger, tcase.partitionedGroupInfo)
			tcase.partitionedGroupInfo.CreationTime = writeRes.CreationTime
			require.NoError(t, err)
			require.Equal(t, tcase.partitionedGroupInfo, *writeRes)
			readRes, converted, err := ReadPartitionedGroupInfo(ctx, bkt, logger, tcase.partitionedGroupInfo.PartitionedGroupID)
			require.NoError(t, err)
			// Make sure when reading the partitioned group info is converted to the latest version.
			require.True(t, converted)
			require.Equal(t, tcase.partitionedGroupInfo.ToPartitionedGroupInfo(), *readRes)
			require.Equal(t, tcase.partitionedGroupInfo.PartitionedGroupID, (*readRes).PartitionedGroupID)
			require.Equal(t, tcase.partitionedGroupInfo.PartitionCount, (*readRes).MetricNamePartitions[0].PartitionCount)
			require.Equal(t, tcase.partitionedGroupInfo.Partitions, (*readRes).MetricNamePartitions[0].Partitions)
		})
	}
}

func TestGetPartitionedGroupStatus(t *testing.T) {
	ulid0 := ulid.MustNew(0, nil)
	ulid1 := ulid.MustNew(1, nil)
	ulid2 := ulid.MustNew(2, nil)
	partitionedGroupID := uint32(1234)
	for _, tcase := range []struct {
		name                   string
		expectedResult         PartitionedGroupStatus
		partitionedGroupInfo   PartitionedGroupInfo
		partitionedGroupInfoV1 PartitionedGroupInfoV1
		// Version of partitioned group info file.
		version                int
		PartitionVisitMarkerV1 []partitionVisitMarker
		PartitionVisitMarkerV2 []PartitionVisitMarkerWithMetricNamePartition
		deletedBlock           map[ulid.ULID]bool
		noCompactBlock         map[ulid.ULID]struct{}
	}{
		{
			name: "partitioned group info v1 with partition visit marker v1, expect new visit marker but couldn't find so status pending",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         false,
				IsCompleted:       false,
				DeleteVisitMarker: false,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						Partition: Partition{
							PartitionID: 0,
							Blocks: []ulid.ULID{
								ulid0,
								ulid1,
							},
						},
					},
				},
			},
			version: PartitionedGroupInfoVersion1,
			partitionedGroupInfoV1: PartitionedGroupInfoV1{
				PartitionedGroupID: partitionedGroupID,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			PartitionVisitMarkerV1: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
		},
		{
			name: "partitioned group info v1 with 1 partition visit marker v1 and 1 partition visit marker v2, completed",
			expectedResult: PartitionedGroupStatus{
				CanDelete:                 true,
				IsCompleted:               true,
				DeleteVisitMarker:         true,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{},
			},
			version: PartitionedGroupInfoVersion1,
			partitionedGroupInfoV1: PartitionedGroupInfoV1{
				PartitionedGroupID: partitionedGroupID,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			PartitionVisitMarkerV1: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID:    partitionedGroupID,
					PartitionID:           0,
					MetricNamePartitionID: 0,
					Status:                Completed,
					VisitTime:             time.Now().Add(-2 * time.Minute).Unix(),
					Version:               PartitionVisitMarkerVersion1,
				},
			},
		},
		{
			name: "test one partition is not visited and contains block marked for deletion",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         true,
				IsCompleted:       false,
				DeleteVisitMarker: true,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						Partition: Partition{
							PartitionID: 1,
							Blocks: []ulid.ULID{
								ulid0,
								ulid2,
							},
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid0: true,
			},
		},
		{
			name: "test one partition is pending and contains block marked for deletion",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         true,
				IsCompleted:       false,
				DeleteVisitMarker: true,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						Partition: Partition{
							PartitionID: 1,
							Blocks: []ulid.ULID{
								ulid0,
								ulid2,
							},
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Pending,
					VisitTime:          time.Now().Add(-5 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid0: true,
			},
		},
		{
			name: "test one partition is completed and one partition is under visiting",
			expectedResult: PartitionedGroupStatus{
				CanDelete:                 false,
				IsCompleted:               false,
				DeleteVisitMarker:         false,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Pending,
					VisitTime:          time.Now().Add(time.Second).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid0: false,
			},
		},
		{
			name: "test one partition is pending expired",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         false,
				IsCompleted:       false,
				DeleteVisitMarker: false,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						Partition: Partition{
							PartitionID: 0,
							Blocks: []ulid.ULID{
								ulid0,
								ulid1,
							},
						},
					},
					{
						Partition: Partition{
							PartitionID: 1,
							Blocks: []ulid.ULID{
								ulid0,
								ulid2,
							},
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Pending,
					VisitTime:          time.Now().Add(-5 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{},
		},
		{
			name: "test one partition is complete with one block deleted and one partition is not visited with no blocks deleted",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         false,
				IsCompleted:       false,
				DeleteVisitMarker: false,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						Partition: Partition{
							PartitionID: 1,
							Blocks: []ulid.ULID{
								ulid0,
								ulid2,
							},
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid1: true,
			},
		},
		{
			name: "test one partition is complete and one partition is failed with no blocks deleted",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         false,
				IsCompleted:       false,
				DeleteVisitMarker: false,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						Partition: Partition{
							PartitionID: 1,
							Blocks: []ulid.ULID{
								ulid0,
								ulid2,
							},
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Failed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{},
		},
		{
			name: "test one partition is complete and one partition is failed one block deleted",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         true,
				IsCompleted:       false,
				DeleteVisitMarker: true,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						Partition: Partition{
							PartitionID: 1,
							Blocks: []ulid.ULID{
								ulid0,
								ulid2,
							},
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Failed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid2: true,
			},
		},
		{
			name: "test all partitions are complete within 1 metric name partition",
			expectedResult: PartitionedGroupStatus{
				CanDelete:                 true,
				IsCompleted:               true,
				DeleteVisitMarker:         true,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid2: true,
			},
		},
		{
			name: "test 2 metric name partition, 1 partition failed, 1 partition pending",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         false,
				IsCompleted:       false,
				DeleteVisitMarker: false,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						MetricNamePartitionID: 0,
						Partition: Partition{
							PartitionID: 1,
							Blocks:      []ulid.ULID{ulid0, ulid2},
						},
					},
					{
						MetricNamePartitionID: 1,
						Partition: Partition{
							PartitionID: 0,
							Blocks:      []ulid.ULID{ulid1, ulid2},
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 2,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
					{
						MetricNamePartitionID: 1,
						PartitionCount:        1,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid1,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Failed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					MetricNamePartitionID: 1,
					PartitionedGroupID:    partitionedGroupID,
					PartitionID:           0,
					Status:                Pending,
					VisitTime:             time.Now().Add(-2 * time.Minute).Unix(),
					Version:               PartitionVisitMarkerVersion1,
				},
			},
		},
		{
			name: "test all partitions are complete from 2 metric name partition",
			expectedResult: PartitionedGroupStatus{
				CanDelete:                 true,
				IsCompleted:               true,
				DeleteVisitMarker:         true,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 2,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
					{
						MetricNamePartitionID: 1,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					MetricNamePartitionID: 1,
					PartitionedGroupID:    partitionedGroupID,
					PartitionID:           0,
					Status:                Completed,
					VisitTime:             time.Now().Add(-2 * time.Minute).Unix(),
					Version:               PartitionVisitMarkerVersion1,
				},
				{
					MetricNamePartitionID: 1,
					PartitionedGroupID:    partitionedGroupID,
					PartitionID:           1,
					Status:                Completed,
					VisitTime:             time.Now().Add(-2 * time.Minute).Unix(),
					Version:               PartitionVisitMarkerVersion1,
				},
			},
		},
		{
			name: "test partitioned group created after visit marker",
			expectedResult: PartitionedGroupStatus{
				CanDelete:                 false,
				IsCompleted:               false,
				DeleteVisitMarker:         true,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(1 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{},
		},
		{
			name: "test one partition is in progress not expired and contains block marked for deletion",
			expectedResult: PartitionedGroupStatus{
				CanDelete:                 false,
				IsCompleted:               false,
				DeleteVisitMarker:         false,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             InProgress,
					VisitTime:          time.Now().Add(time.Second).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid0: true,
			},
		},
		{
			name: "test one partition is not visited and contains block with no compact mark",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         true,
				IsCompleted:       false,
				DeleteVisitMarker: true,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						Partition: Partition{
							PartitionID: 1,
							Blocks: []ulid.ULID{
								ulid0,
								ulid2,
							},
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			noCompactBlock: map[ulid.ULID]struct{}{
				ulid0: {},
			},
		},
		{
			name: "test one partition is expired and contains block with no compact mark",
			expectedResult: PartitionedGroupStatus{
				CanDelete:         true,
				IsCompleted:       false,
				DeleteVisitMarker: true,
				PendingOrFailedPartitions: []PartitionWithMetricNamePartitionID{
					{
						Partition: Partition{
							PartitionID: 1,
							Blocks: []ulid.ULID{
								ulid0,
								ulid2,
							},
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID:       partitionedGroupID,
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{
								PartitionID: 0,
								Blocks: []ulid.ULID{
									ulid0,
									ulid1,
								},
							},
							{
								PartitionID: 1,
								Blocks: []ulid.ULID{
									ulid0,
									ulid2,
								},
							},
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion2,
			},
			PartitionVisitMarkerV2: []PartitionVisitMarkerWithMetricNamePartition{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             InProgress,
					VisitTime:          time.Now().Add(-10 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			noCompactBlock: map[ulid.ULID]struct{}{
				ulid0: {},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bucketClient := &bucket.ClientMock{}
			for _, p := range tcase.PartitionVisitMarkerV2 {
				content, _ := json.Marshal(p)
				bucketClient.MockGet(p.GetVisitMarkerFilePath(), string(content), nil)
			}

			for _, p := range tcase.PartitionVisitMarkerV1 {
				content, _ := json.Marshal(p)
				bucketClient.MockGet(p.GetVisitMarkerFilePath(), string(content), nil)
			}

			handleBlockFunc := func(blockID ulid.ULID) {
				metaPath := path.Join(blockID.String(), metadata.MetaFilename)
				noCompactPath := path.Join(blockID.String(), metadata.NoCompactMarkFilename)
				deletionMarkerPath := path.Join(blockID.String(), metadata.DeletionMarkFilename)
				if hasDeletionMarker, ok := tcase.deletedBlock[blockID]; ok {
					if hasDeletionMarker {
						bucketClient.MockExists(metaPath, true, nil)
						bucketClient.MockExists(deletionMarkerPath, true, nil)
					} else {
						bucketClient.MockExists(metaPath, false, nil)
					}
				} else {
					bucketClient.MockExists(metaPath, true, nil)
					bucketClient.MockExists(deletionMarkerPath, false, nil)
				}
				if _, ok := tcase.noCompactBlock[blockID]; ok {
					bucketClient.MockExists(noCompactPath, true, nil)
				} else {
					bucketClient.MockExists(noCompactPath, false, nil)
				}
			}

			if tcase.version == 0 {
				tcase.version = PartitionedGroupInfoVersion2
			}
			var partitionedGroupInfo PartitionedGroupInfo
			if tcase.version == PartitionedGroupInfoVersion2 {
				for _, metricNamePartition := range tcase.partitionedGroupInfo.MetricNamePartitions {
					for _, partition := range metricNamePartition.Partitions {
						for _, blockID := range partition.Blocks {
							handleBlockFunc(blockID)
						}
					}
				}
				partitionedGroupInfo = tcase.partitionedGroupInfo
			} else {
				for _, partition := range tcase.partitionedGroupInfoV1.Partitions {
					for _, blockID := range partition.Blocks {
						handleBlockFunc(blockID)
					}
				}
				partitionedGroupInfo = tcase.partitionedGroupInfoV1.ToPartitionedGroupInfo()
			}

			bucketClient.MockGet(mock.Anything, "", nil)

			ctx := context.Background()
			logger := log.NewNopLogger()
			result := partitionedGroupInfo.getPartitionedGroupStatus(ctx, bucketClient, 60*time.Second, logger)
			require.Equal(t, tcase.expectedResult.CanDelete, result.CanDelete)
			require.Equal(t, tcase.expectedResult.IsCompleted, result.IsCompleted)
			require.Equal(t, len(tcase.expectedResult.PendingOrFailedPartitions), len(result.PendingOrFailedPartitions))
			for _, partition := range result.PendingOrFailedPartitions {
				require.Contains(t, tcase.expectedResult.PendingOrFailedPartitions, partition)
			}
		})
	}
}

func TestPartitionedGroupInfoGetAllBlocks(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	block4 := ulid.MustNew(4, nil)
	for _, tc := range []struct {
		name                 string
		partitionedGroupInfo *PartitionedGroupInfo
		expectedBlocks       []ulid.ULID
	}{
		{
			name:                 "empty",
			partitionedGroupInfo: &PartitionedGroupInfo{},
		},
		{
			name: "one metric name partition but empty",
			partitionedGroupInfo: &PartitionedGroupInfo{
				MetricNamePartitions: []MetricNamePartition{{}},
			},
			expectedBlocks: []ulid.ULID{},
		},
		{
			name: "one metric name partition and one empty partition",
			partitionedGroupInfo: &PartitionedGroupInfo{
				MetricNamePartitions: []MetricNamePartition{{
					Partitions: []Partition{},
				}},
			},
			expectedBlocks: []ulid.ULID{},
		},
		{
			name: "one metric name partition and one partition with one block",
			partitionedGroupInfo: &PartitionedGroupInfo{
				MetricNamePartitions: []MetricNamePartition{{
					Partitions: []Partition{
						{Blocks: []ulid.ULID{block1}},
					},
				}},
			},
			expectedBlocks: []ulid.ULID{block1},
		},
		{
			name: "one metric name partition and one partition with two block",
			partitionedGroupInfo: &PartitionedGroupInfo{
				MetricNamePartitions: []MetricNamePartition{{
					Partitions: []Partition{
						{Blocks: []ulid.ULID{block1, block2}},
					},
				}},
			},
			expectedBlocks: []ulid.ULID{block1, block2},
		},
		{
			name: "one metric name partition and two partitions with different blocks",
			partitionedGroupInfo: &PartitionedGroupInfo{
				MetricNamePartitions: []MetricNamePartition{{
					Partitions: []Partition{
						{Blocks: []ulid.ULID{block1, block2}},
						{Blocks: []ulid.ULID{block3, block4}},
					},
				}},
			},
			expectedBlocks: []ulid.ULID{block1, block2, block3, block4},
		},
		{
			name: "one metric name partition with same blocks",
			partitionedGroupInfo: &PartitionedGroupInfo{
				MetricNamePartitions: []MetricNamePartition{{
					Partitions: []Partition{
						{Blocks: []ulid.ULID{block1, block2}},
						{Blocks: []ulid.ULID{block3, block4}},
						{Blocks: []ulid.ULID{block1, block3}},
						{Blocks: []ulid.ULID{block2, block4}},
					},
				}},
			},
			expectedBlocks: []ulid.ULID{block1, block2, block3, block4},
		},
		{
			name: "two metric name partition with different blocks",
			partitionedGroupInfo: &PartitionedGroupInfo{
				MetricNamePartitions: []MetricNamePartition{
					{
						Partitions: []Partition{
							{Blocks: []ulid.ULID{block1, block2}},
						},
					},
					{
						Partitions: []Partition{
							{Blocks: []ulid.ULID{block3, block4}},
						},
					},
				},
			},
			expectedBlocks: []ulid.ULID{block1, block2, block3, block4},
		},
		{
			name: "two metric name partition with same blocks",
			partitionedGroupInfo: &PartitionedGroupInfo{
				MetricNamePartitions: []MetricNamePartition{
					{
						Partitions: []Partition{
							{Blocks: []ulid.ULID{block1, block2, block3, block4}},
						},
					},
					{
						Partitions: []Partition{
							{Blocks: []ulid.ULID{block1, block2, block3, block4}},
						},
					},
				},
			},
			expectedBlocks: []ulid.ULID{block1, block2, block3, block4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.partitionedGroupInfo.getAllBlocks()
			sort.Slice(actual, func(i, j int) bool {
				return actual[i].Compare(actual[j]) < 0
			})

			sort.Slice(tc.expectedBlocks, func(i, j int) bool {
				return tc.expectedBlocks[i].Compare(tc.expectedBlocks[j]) < 0
			})
			require.Equal(t, tc.expectedBlocks, actual)
		})
	}
}

func TestReadPartitionedGroupInfoFile(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	groupFilePath := "group_file_path"
	testErr := errors.New("test error")
	for _, tc := range []struct {
		name                         string
		prepare                      func(b *bucket.ClientMock)
		expectedErr                  error
		expectedConverted            bool
		expectedPartitionedGroupInfo *PartitionedGroupInfo
		expectedReadFailure          float64
	}{
		{
			name: "partition group info file doesn't exist",
			prepare: func(b *bucket.ClientMock) {
				b.MockGet(groupFilePath, "", nil)
			},
			expectedErr: errors.Wrapf(ErrorPartitionedGroupInfoNotFound, "partitioned group file: %s", groupFilePath),
		},
		{
			name: "partition group info file read failed",
			prepare: func(b *bucket.ClientMock) {
				b.MockGet(groupFilePath, "1", testErr)
			},
			expectedReadFailure: 1,
			expectedErr:         errors.Wrapf(testErr, "get partitioned group file: %s", groupFilePath),
		},
		{
			name: "invalid partition group info file format",
			prepare: func(b *bucket.ClientMock) {
				b.MockGet(groupFilePath, "1", nil)
			},
			expectedReadFailure: 1,
			expectedErr:         errors.Wrapf(ErrorUnmarshalPartitionedGroupInfo, "partitioned group file: %s, error: json: cannot unmarshal number into Go value of type compactor.version", groupFilePath),
		},
		{
			name: "unsupported partition info file version",
			prepare: func(b *bucket.ClientMock) {
				type version struct {
					// Version of the file.
					Version int `json:"version"`
				}
				out, err := json.Marshal(version{Version: 3})
				require.NoError(t, err)
				b.MockGet(groupFilePath, string(out), nil)
			},
			expectedReadFailure: 1,
			expectedErr:         errors.Errorf("unexpected partitioned group file version 3, expected %v", SupportedPartitionedGroupInfoVersions),
		},
		{
			name: "partition info file version 1, but failed to unmarshal",
			prepare: func(b *bucket.ClientMock) {
				type version struct {
					// Version of the file.
					Version        int      `json:"version"`
					PartitionCount []string `json:"partitionCount"`
				}
				out, err := json.Marshal(version{Version: 1, PartitionCount: []string{"1", "2"}})
				require.NoError(t, err)
				b.MockGet(groupFilePath, string(out), nil)
			},
			expectedReadFailure: 1,
			expectedErr:         errors.Wrapf(ErrorUnmarshalPartitionedGroupInfo, "partitioned group file: %s, error: json: cannot unmarshal array into Go struct field PartitionedGroupInfoV1.partitionCount of type int", groupFilePath),
		},
		{
			name: "partition info file version 2, but failed to unmarshal",
			prepare: func(b *bucket.ClientMock) {
				type version struct {
					// Version of the file.
					Version        int      `json:"version"`
					PartitionCount []string `json:"metricNamePartitionCount"`
				}
				out, err := json.Marshal(version{Version: 2, PartitionCount: []string{"1", "2"}})
				require.NoError(t, err)
				b.MockGet(groupFilePath, string(out), nil)
			},
			expectedReadFailure: 1,
			expectedErr:         errors.Wrapf(ErrorUnmarshalPartitionedGroupInfo, "partitioned group file: %s, error: json: cannot unmarshal array into Go struct field PartitionedGroupInfo.metricNamePartitionCount of type int", groupFilePath),
		},
		{
			name: "partition info file version 1, successfully unmarshalled and converted",
			prepare: func(b *bucket.ClientMock) {
				p := &PartitionedGroupInfoV1{
					PartitionedGroupID: uint32(10),
					PartitionCount:     2,
					Partitions: []Partition{
						{PartitionID: 0},
						{PartitionID: 1},
					},
					CreationTime: 1,
					RangeStart:   1,
					RangeEnd:     2,
					Version:      PartitionedGroupInfoVersion1,
				}
				out, err := json.Marshal(p)
				require.NoError(t, err)
				b.MockGet(groupFilePath, string(out), nil)
			},
			expectedPartitionedGroupInfo: &PartitionedGroupInfo{
				PartitionedGroupID:       uint32(10),
				MetricNamePartitionCount: 1,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{PartitionID: 0},
							{PartitionID: 1},
						},
					},
				},
				CreationTime: 1,
				RangeStart:   1,
				RangeEnd:     2,
				Version:      PartitionedGroupInfoVersion2,
			},
			expectedConverted: true,
		},
		{
			name: "partition info file version 2, successfully unmarshalled",
			prepare: func(b *bucket.ClientMock) {
				p := &PartitionedGroupInfo{
					PartitionedGroupID:       uint32(10),
					MetricNamePartitionCount: 2,
					MetricNamePartitions: []MetricNamePartition{
						{
							MetricNamePartitionID: 0,
							PartitionCount:        2,
							Partitions: []Partition{
								{PartitionID: 0},
								{PartitionID: 1},
							},
						},
						{
							MetricNamePartitionID: 1,
							PartitionCount:        2,
							Partitions: []Partition{
								{PartitionID: 0},
								{PartitionID: 1},
							},
						},
					},
					CreationTime: 1,
					RangeStart:   1,
					RangeEnd:     2,
					Version:      PartitionedGroupInfoVersion2,
				}

				out, err := json.Marshal(p)
				require.NoError(t, err)
				b.MockGet(groupFilePath, string(out), nil)
			},
			expectedPartitionedGroupInfo: &PartitionedGroupInfo{
				PartitionedGroupID:       uint32(10),
				MetricNamePartitionCount: 2,
				MetricNamePartitions: []MetricNamePartition{
					{
						MetricNamePartitionID: 0,
						PartitionCount:        2,
						Partitions: []Partition{
							{PartitionID: 0},
							{PartitionID: 1},
						},
					},
					{
						MetricNamePartitionID: 1,
						PartitionCount:        2,
						Partitions: []Partition{
							{PartitionID: 0},
							{PartitionID: 1},
						},
					},
				},
				CreationTime: 1,
				RangeStart:   1,
				RangeEnd:     2,
				Version:      PartitionedGroupInfoVersion2,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucketClient := &bucket.ClientMock{}
			tc.prepare(bucketClient)
			partitionedGroupInfo, converted, err := ReadPartitionedGroupInfoFile(ctx, bucketClient, logger, groupFilePath)
			if tc.expectedErr != nil {
				require.EqualError(t, err, tc.expectedErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedConverted, converted)
				require.Equal(t, tc.expectedPartitionedGroupInfo, partitionedGroupInfo)
			}
		})
	}
}

func TestUpdatePartitionedGroupInfo(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	pgID := uint32(10)
	filePath := GetPartitionedGroupFile(pgID)
	testErr := errors.New("test error")
	testPartitionGroupInfoV2 := &PartitionedGroupInfo{
		PartitionedGroupID:       pgID,
		MetricNamePartitionCount: 1,
		MetricNamePartitions: []MetricNamePartition{
			{
				MetricNamePartitionID: 0,
				PartitionCount:        2,
				Partitions:            []Partition{{PartitionID: 0}, {PartitionID: 1}},
			},
		},
		CreationTime: 1,
		RangeStart:   1,
		RangeEnd:     2,
		Version:      PartitionedGroupInfoVersion2,
	}
	testPartitionGroupInfoV2Content, err := json.Marshal(testPartitionGroupInfoV2)
	require.NoError(t, err)
	testPartitionGroupInfoV1 := &PartitionedGroupInfoV1{
		PartitionedGroupID: pgID,
		PartitionCount:     2,
		Partitions: []Partition{
			{PartitionID: 0},
			{PartitionID: 1},
		},
		CreationTime: 1,
		RangeStart:   1,
		RangeEnd:     2,
		Version:      PartitionedGroupInfoVersion1,
	}
	testPartitionGroupInfoV1Content, err := json.Marshal(testPartitionGroupInfoV1)
	require.NoError(t, err)

	for _, tc := range []struct {
		name                 string
		prepare              func(b *bucket.ClientMock)
		p                    PartitionedGroupInfo
		expectedErr          error
		expectedReadFailure  float64
		expectedWriteFailure float64
	}{
		{
			name: "partition group info file v2 read success",
			prepare: func(b *bucket.ClientMock) {
				b.MockGet(filePath, string(testPartitionGroupInfoV2Content), nil)
			},
			p: *testPartitionGroupInfoV2,
		},
		{
			name: "partition group info file doesn't exist, upload file",
			prepare: func(b *bucket.ClientMock) {
				b.MockGet(filePath, "", nil)
				b.MockUpload(filePath, nil)
			},
			p: *testPartitionGroupInfoV2,
		},
		{
			name: "partition group info file read failed, upload file",
			prepare: func(b *bucket.ClientMock) {
				b.MockGet(filePath, "1", testErr)
				b.MockUpload(filePath, nil)
			},
			p:                   *testPartitionGroupInfoV2,
			expectedReadFailure: 1,
		},
		{
			name: "partition group info file read failed, upload file failed",
			prepare: func(b *bucket.ClientMock) {
				b.MockGet(filePath, "1", testErr)
				b.MockUpload(filePath, testErr)
			},
			p:                    *testPartitionGroupInfoV2,
			expectedReadFailure:  1,
			expectedWriteFailure: 1,
			expectedErr:          errors.Wrapf(testErr, "unable to upload partitioned group file: %s", filePath),
		},
		{
			name: "partition group info file v1 read and upload success",
			prepare: func(b *bucket.ClientMock) {
				b.MockGet(filePath, string(testPartitionGroupInfoV1Content), nil)
				b.MockUpload(filePath, nil)
			},
			p: *testPartitionGroupInfoV2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucketClient := &bucket.ClientMock{}
			tc.prepare(bucketClient)
			actual, err := UpdatePartitionedGroupInfo(ctx, bucketClient, logger, tc.p)

			if tc.expectedErr != nil {
				require.EqualError(t, err, tc.expectedErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, &tc.p, actual)
			}
		})
	}
}
