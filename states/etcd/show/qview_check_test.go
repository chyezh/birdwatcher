package show

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

func TestBuildQueryViewCheckIssuesDetectsSegmentAndRecoveryDrift(t *testing.T) {
	coord := common.QueryViewMetaEntry{
		Key:    "by-dev/meta/coord/qv/100/1/ch-1/10/20/3",
		Prefix: "by-dev/meta/coord/qv/",
		View:   testQueryView(100, 1, "ch-1", 10, 20, 3, common.QueryViewStateUp, [][]int64{{10, 10, 99}}),
	}
	data := common.DataViewMetaEntry{
		Key:      "by-dev/meta/datacoord-meta/dataview/100/versions/10/20",
		Prefix:   "by-dev/meta/datacoord-meta/dataview/",
		DataView: testDataView(100, "ch-1", 10, 20, []int64{10, 11}),
	}

	issues := buildQueryViewCheckIssues([]common.QueryViewMetaEntry{coord}, []common.DataViewMetaEntry{data}, nil)
	issueTypes := make([]string, 0, len(issues))
	for _, issue := range issues {
		issueTypes = append(issueTypes, issue.Type)
	}

	require.ElementsMatch(t, []string{
		"DuplicateSegmentAssignment",
		"QueryViewSegmentNotInDataView",
		"DataViewSegmentNotAssignedToQueryView",
		"CoordUpQueryViewMissingRecovery",
	}, issueTypes)
}

func TestBuildQueryViewCheckIssuesAcceptsConsistentQueryViewMeta(t *testing.T) {
	coord := common.QueryViewMetaEntry{
		Key:    "by-dev/meta/coord/qv/100/1/ch-1/10/20/3",
		Prefix: "by-dev/meta/coord/qv/",
		View:   testQueryView(100, 1, "ch-1", 10, 20, 3, common.QueryViewStateUp, [][]int64{{10, 11}}),
	}
	data := common.DataViewMetaEntry{
		Key:      "by-dev/meta/datacoord-meta/dataview/100/versions/10/20",
		Prefix:   "by-dev/meta/datacoord-meta/dataview/",
		DataView: testDataView(100, "ch-1", 10, 20, []int64{10, 11}),
	}
	recovery := common.QueryViewMetaEntry{
		Key:      "by-dev/meta/streamingnode-meta/wal/pch-1/query-view/100/1/ch-1/10/20/3",
		Prefix:   "by-dev/meta/streamingnode-meta/wal/",
		PChannel: "pch-1",
		View:     testQueryView(100, 1, "ch-1", 10, 20, 3, common.QueryViewStateUp, [][]int64{{10, 11}}),
	}

	require.Empty(t, buildQueryViewCheckIssues([]common.QueryViewMetaEntry{coord}, []common.DataViewMetaEntry{data}, []common.QueryViewMetaEntry{recovery}))
}

func testQueryView(collectionID, replicaID int64, vchannel string, streamingVersion, compactVersion, queryVersion int64, state common.QueryViewState, segmentGroups [][]int64) *common.QueryViewOfShard {
	qnodes := make([]*common.QueryViewOfQueryNode, 0, len(segmentGroups))
	for i, segments := range segmentGroups {
		qnodes = append(qnodes, &common.QueryViewOfQueryNode{
			NodeId: int64(i + 1),
			Partitions: []*common.QueryViewOfPartition{{
				PartitionId: 1,
				SegmentIds:  append([]int64(nil), segments...),
			}},
		})
	}
	return &common.QueryViewOfShard{
		Meta: &common.QueryViewMeta{
			CollectionId: collectionID,
			ReplicaId:    replicaID,
			Vchannel:     vchannel,
			State:        state,
			Version: &common.QueryViewVersion{
				DataVersion: &common.DataVersion{
					StreamingVersion: streamingVersion,
					CompactVersion:   compactVersion,
				},
				QueryVersion: queryVersion,
			},
		},
		QueryNode:     qnodes,
		StreamingNode: &common.QueryViewOfStreamingNode{},
	}
}

func testDataView(collectionID int64, vchannel string, streamingVersion, compactVersion int64, segments []int64) *common.DataViewOfCollection {
	return &common.DataViewOfCollection{
		CollectionId: collectionID,
		DataVersion: &common.DataVersion{
			StreamingVersion: streamingVersion,
			CompactVersion:   compactVersion,
		},
		Shards: []*common.DataViewOfShard{{
			Vchannel: vchannel,
			Partitions: []*common.DataViewOfPartition{{
				PartitionId: 1,
				SegmentIds:  append([]int64(nil), segments...),
			}},
		}},
	}
}
