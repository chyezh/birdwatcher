package common

import (
	"context"
	"path"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestQueryViewWireRoundTrip(t *testing.T) {
	view := &QueryViewOfShard{
		Meta: &QueryViewMeta{
			CollectionId: 100,
			ReplicaId:    2,
			Vchannel:     "ch-1",
			State:        QueryViewStateUp,
			Version: &QueryViewVersion{
				DataVersion:  &DataVersion{StreamingVersion: 10, CompactVersion: 20},
				QueryVersion: 3,
			},
			LoadInfoVersion:             7,
			TransformStartAfterTimetick: 99,
		},
		QueryNode: []*QueryViewOfQueryNode{{
			NodeId: 11,
			Partitions: []*QueryViewOfPartition{{
				PartitionId:     1,
				SegmentIds:      []int64{101, 102},
				ReadySegmentIds: []int64{101},
			}},
		}},
		StreamingNode: &QueryViewOfStreamingNode{},
	}

	data, err := proto.Marshal(view)
	require.NoError(t, err)

	var decoded QueryViewOfShard
	require.NoError(t, proto.Unmarshal(data, &decoded))
	require.True(t, CoordQueryViewKeyMatches("by-dev/meta/coord/qv/", "by-dev/meta/coord/qv/100/2/ch-1/10/20/3", decoded.GetMeta()))
	require.Equal(t, []int64{101, 102}, decoded.GetQueryNode()[0].GetPartitions()[0].GetSegmentIds())
	require.Equal(t, []int64{101}, decoded.GetQueryNode()[0].GetPartitions()[0].GetReadySegmentIds())
}

func TestListCoordQueryViewsUsesCoordQvPrefix(t *testing.T) {
	ctx := context.Background()
	metaPath := "by-dev/meta"
	view := &QueryViewOfShard{
		Meta: &QueryViewMeta{
			CollectionId: 100,
			ReplicaId:    2,
			Vchannel:     "ch-1",
			State:        QueryViewStateUp,
			Version: &QueryViewVersion{
				DataVersion:  &DataVersion{StreamingVersion: 10, CompactVersion: 20},
				QueryVersion: 3,
			},
		},
	}
	value, err := proto.Marshal(view)
	require.NoError(t, err)
	key := path.Join(metaPath, "coord/qv", "100", "2", "ch-1", "10", "20", "3")
	cli := &segmentListKV{data: map[string]string{key: string(value)}}

	views, err := ListCoordQueryViews(ctx, cli, metaPath)
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(metaPath, "coord/qv") + "/"}, cli.loadedPrefixes)
	require.Len(t, views, 1)
	require.Equal(t, key, views[0].Key)
	require.EqualValues(t, 100, views[0].View.GetMeta().GetCollectionId())
}

func TestListCoordQueryViewsUsesCollectionPrefix(t *testing.T) {
	ctx := context.Background()
	metaPath := "by-dev/meta"
	value100 := mustQueryViewValue(t, 100, "ch-1")
	value200 := mustQueryViewValue(t, 200, "ch-2")
	cli := &segmentListKV{data: map[string]string{
		path.Join(metaPath, "coord/qv", "100", "2", "ch-1", "10", "20", "3"): value100,
		path.Join(metaPath, "coord/qv", "200", "2", "ch-2", "10", "20", "3"): value200,
	}}

	views, err := ListCoordQueryViewsByCollection(ctx, cli, metaPath, 100)
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(metaPath, "coord/qv", "100") + "/"}, cli.loadedPrefixes)
	require.Len(t, views, 1)
	require.EqualValues(t, 100, views[0].View.GetMeta().GetCollectionId())
}

func TestListDataViewsUsesCollectionPrefix(t *testing.T) {
	ctx := context.Background()
	metaPath := "by-dev/meta"
	value100 := mustDataViewValue(t, 100, "ch-1")
	value200 := mustDataViewValue(t, 200, "ch-2")
	cli := &segmentListKV{data: map[string]string{
		path.Join(metaPath, "datacoord-meta/dataview", "100", "versions", "10", "20"): value100,
		path.Join(metaPath, "datacoord-meta/dataview", "200", "versions", "10", "20"): value200,
	}}

	views, err := ListDataViewsByCollection(ctx, cli, metaPath, 100)
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(metaPath, "datacoord-meta/dataview", "100", "versions") + "/"}, cli.loadedPrefixes)
	require.Len(t, views, 1)
	require.EqualValues(t, 100, views[0].DataView.GetCollectionId())
}

func TestListStreamingNodeQueryViewRecoveryUsesPChannelCollectionPrefix(t *testing.T) {
	ctx := context.Background()
	metaPath := "by-dev/meta"
	value100 := mustQueryViewValue(t, 100, "ch-1")
	value200 := mustQueryViewValue(t, 200, "ch-2")
	cli := &segmentListKV{data: map[string]string{
		path.Join(metaPath, "streamingnode-meta/wal", "p1", "query-view", "100", "2", "ch-1", "10", "20", "3"): value100,
		path.Join(metaPath, "streamingnode-meta/wal", "p1", "query-view", "200", "2", "ch-2", "10", "20", "3"): value200,
		path.Join(metaPath, "streamingnode-meta/wal", "p2", "query-view", "100", "2", "ch-1", "10", "20", "3"): value100,
	}}

	views, err := ListStreamingNodeQueryViewRecoveryBy(ctx, cli, metaPath, "p1", 100)
	require.NoError(t, err)
	require.Equal(t, []string{path.Join(metaPath, "streamingnode-meta/wal", "p1", "query-view", "100") + "/"}, cli.loadedPrefixes)
	require.Len(t, views, 1)
	require.Equal(t, "p1", views[0].PChannel)
	require.EqualValues(t, 100, views[0].View.GetMeta().GetCollectionId())
}

func TestDataViewWireRoundTrip(t *testing.T) {
	view := &DataViewOfCollection{
		CollectionId: 100,
		DataVersion:  &DataVersion{StreamingVersion: 10, CompactVersion: 20},
		Shards: []*DataViewOfShard{{
			Vchannel:                    "ch-1",
			TransformStartAfterTimetick: 99,
			Partitions: []*DataViewOfPartition{{
				PartitionId: 1,
				SegmentIds:  []int64{101, 102},
			}},
		}},
	}

	data, err := proto.Marshal(view)
	require.NoError(t, err)

	var decoded DataViewOfCollection
	require.NoError(t, proto.Unmarshal(data, &decoded))
	require.True(t, DataViewKeyMatches("by-dev/meta/datacoord-meta/dataview/", "by-dev/meta/datacoord-meta/dataview/100/versions/10/20", &decoded))
	require.Equal(t, uint64(99), decoded.GetShards()[0].GetTransformStartAfterTimetick())
	require.Equal(t, []int64{101, 102}, decoded.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func mustQueryViewValue(t *testing.T, collectionID int64, vchannel string) string {
	t.Helper()
	data, err := proto.Marshal(&QueryViewOfShard{
		Meta: &QueryViewMeta{
			CollectionId: collectionID,
			ReplicaId:    2,
			Vchannel:     vchannel,
			State:        QueryViewStateUp,
			Version: &QueryViewVersion{
				DataVersion:  &DataVersion{StreamingVersion: 10, CompactVersion: 20},
				QueryVersion: 3,
			},
		},
	})
	require.NoError(t, err)
	return string(data)
}

func mustDataViewValue(t *testing.T, collectionID int64, vchannel string) string {
	t.Helper()
	data, err := proto.Marshal(&DataViewOfCollection{
		CollectionId: collectionID,
		DataVersion:  &DataVersion{StreamingVersion: 10, CompactVersion: 20},
		Shards: []*DataViewOfShard{{
			Vchannel: vchannel,
		}},
	})
	require.NoError(t, err)
	return string(data)
}
