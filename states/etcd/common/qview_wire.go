package common

import "fmt"

type DataVersion struct {
	StreamingVersion int64 `protobuf:"varint,1,opt,name=streaming_version,json=streamingVersion,proto3" json:"streaming_version,omitempty"`
	CompactVersion   int64 `protobuf:"varint,2,opt,name=compact_version,json=compactVersion,proto3" json:"compact_version,omitempty"`
}

func (*DataVersion) Reset()        {}
func (*DataVersion) ProtoMessage() {}
func (m *DataVersion) String() string {
	return fmt.Sprintf("%d/%d", m.GetStreamingVersion(), m.GetCompactVersion())
}

func (m *DataVersion) GetStreamingVersion() int64 {
	if m != nil {
		return m.StreamingVersion
	}
	return 0
}

func (m *DataVersion) GetCompactVersion() int64 {
	if m != nil {
		return m.CompactVersion
	}
	return 0
}

type DataViewOfCollection struct {
	CollectionId int64              `protobuf:"varint,1,opt,name=collection_id,json=collectionId,proto3" json:"collection_id,omitempty"`
	Shards       []*DataViewOfShard `protobuf:"bytes,2,rep,name=shards,proto3" json:"shards,omitempty"`
	DataVersion  *DataVersion       `protobuf:"bytes,3,opt,name=data_version,json=dataVersion,proto3" json:"data_version,omitempty"`
}

func (*DataViewOfCollection) Reset()        {}
func (*DataViewOfCollection) ProtoMessage() {}
func (m *DataViewOfCollection) String() string {
	return fmt.Sprintf("collection=%d version=%s", m.GetCollectionId(), m.GetDataVersion())
}

func (m *DataViewOfCollection) GetCollectionId() int64 {
	if m != nil {
		return m.CollectionId
	}
	return 0
}

func (m *DataViewOfCollection) GetShards() []*DataViewOfShard {
	if m != nil {
		return m.Shards
	}
	return nil
}

func (m *DataViewOfCollection) GetDataVersion() *DataVersion {
	if m != nil {
		return m.DataVersion
	}
	return nil
}

type DataViewOfShard struct {
	Vchannel                    string                 `protobuf:"bytes,1,opt,name=vchannel,proto3" json:"vchannel,omitempty"`
	Partitions                  []*DataViewOfPartition `protobuf:"bytes,2,rep,name=partitions,proto3" json:"partitions,omitempty"`
	TransformStartAfterTimetick uint64                 `protobuf:"varint,3,opt,name=transform_start_after_timetick,json=transformStartAfterTimetick,proto3" json:"transform_start_after_timetick,omitempty"`
}

func (m *DataViewOfShard) GetVchannel() string {
	if m != nil {
		return m.Vchannel
	}
	return ""
}

func (m *DataViewOfShard) GetPartitions() []*DataViewOfPartition {
	if m != nil {
		return m.Partitions
	}
	return nil
}

func (m *DataViewOfShard) GetTransformStartAfterTimetick() uint64 {
	if m != nil {
		return m.TransformStartAfterTimetick
	}
	return 0
}

type DataViewOfPartition struct {
	PartitionId int64   `protobuf:"varint,1,opt,name=partition_id,json=partitionId,proto3" json:"partition_id,omitempty"`
	SegmentIds  []int64 `protobuf:"varint,2,rep,packed,name=segment_ids,json=segmentIds,proto3" json:"segment_ids,omitempty"`
}

func (m *DataViewOfPartition) GetPartitionId() int64 {
	if m != nil {
		return m.PartitionId
	}
	return 0
}

func (m *DataViewOfPartition) GetSegmentIds() []int64 {
	if m != nil {
		return m.SegmentIds
	}
	return nil
}

type QueryViewState int32

const (
	QueryViewStateUnknown       QueryViewState = 0
	QueryViewStatePreparing     QueryViewState = 1
	QueryViewStateReady         QueryViewState = 2
	QueryViewStateUp            QueryViewState = 3
	QueryViewStateDown          QueryViewState = 4
	QueryViewStateUnrecoverable QueryViewState = 5
	QueryViewStateDropping      QueryViewState = 6
	QueryViewStateDropped       QueryViewState = 7
	QueryViewStateUpRecovering  QueryViewState = 8
)

func (s QueryViewState) String() string {
	switch s {
	case QueryViewStatePreparing:
		return "QueryViewStatePreparing"
	case QueryViewStateReady:
		return "QueryViewStateReady"
	case QueryViewStateUp:
		return "QueryViewStateUp"
	case QueryViewStateDown:
		return "QueryViewStateDown"
	case QueryViewStateUnrecoverable:
		return "QueryViewStateUnrecoverable"
	case QueryViewStateDropping:
		return "QueryViewStateDropping"
	case QueryViewStateDropped:
		return "QueryViewStateDropped"
	case QueryViewStateUpRecovering:
		return "QueryViewStateUpRecovering"
	default:
		return "QueryViewStateUnknown"
	}
}

type QueryViewOfShard struct {
	Meta          *QueryViewMeta            `protobuf:"bytes,1,opt,name=meta,proto3" json:"meta,omitempty"`
	QueryNode     []*QueryViewOfQueryNode   `protobuf:"bytes,2,rep,name=query_node,json=queryNode,proto3" json:"query_node,omitempty"`
	StreamingNode *QueryViewOfStreamingNode `protobuf:"bytes,3,opt,name=streaming_node,json=streamingNode,proto3" json:"streaming_node,omitempty"`
}

func (*QueryViewOfShard) Reset()        {}
func (*QueryViewOfShard) ProtoMessage() {}
func (m *QueryViewOfShard) String() string {
	return fmt.Sprintf("collection=%d replica=%d vchannel=%s version=%s", m.GetMeta().GetCollectionId(), m.GetMeta().GetReplicaId(), m.GetMeta().GetVchannel(), m.GetMeta().GetVersion())
}

func (m *QueryViewOfShard) GetMeta() *QueryViewMeta {
	if m != nil {
		return m.Meta
	}
	return nil
}

func (m *QueryViewOfShard) GetQueryNode() []*QueryViewOfQueryNode {
	if m != nil {
		return m.QueryNode
	}
	return nil
}

func (m *QueryViewOfShard) GetStreamingNode() *QueryViewOfStreamingNode {
	if m != nil {
		return m.StreamingNode
	}
	return nil
}

type QueryViewMeta struct {
	CollectionId                int64             `protobuf:"varint,1,opt,name=collection_id,json=collectionId,proto3" json:"collection_id,omitempty"`
	ReplicaId                   int64             `protobuf:"varint,2,opt,name=replica_id,json=replicaId,proto3" json:"replica_id,omitempty"`
	Vchannel                    string            `protobuf:"bytes,3,opt,name=vchannel,proto3" json:"vchannel,omitempty"`
	Version                     *QueryViewVersion `protobuf:"bytes,4,opt,name=version,proto3" json:"version,omitempty"`
	State                       QueryViewState    `protobuf:"varint,5,opt,name=state,proto3,enum=milvus.proto.view.QueryViewState" json:"state,omitempty"`
	LoadInfoVersion             uint64            `protobuf:"varint,6,opt,name=load_info_version,json=loadInfoVersion,proto3" json:"load_info_version,omitempty"`
	TransformStartAfterTimetick uint64            `protobuf:"varint,7,opt,name=transform_start_after_timetick,json=transformStartAfterTimetick,proto3" json:"transform_start_after_timetick,omitempty"`
}

func (m *QueryViewMeta) GetCollectionId() int64 {
	if m != nil {
		return m.CollectionId
	}
	return 0
}

func (m *QueryViewMeta) GetReplicaId() int64 {
	if m != nil {
		return m.ReplicaId
	}
	return 0
}

func (m *QueryViewMeta) GetVchannel() string {
	if m != nil {
		return m.Vchannel
	}
	return ""
}

func (m *QueryViewMeta) GetVersion() *QueryViewVersion {
	if m != nil {
		return m.Version
	}
	return nil
}

func (m *QueryViewMeta) GetState() QueryViewState {
	if m != nil {
		return m.State
	}
	return QueryViewStateUnknown
}

func (m *QueryViewMeta) GetLoadInfoVersion() uint64 {
	if m != nil {
		return m.LoadInfoVersion
	}
	return 0
}

func (m *QueryViewMeta) GetTransformStartAfterTimetick() uint64 {
	if m != nil {
		return m.TransformStartAfterTimetick
	}
	return 0
}

type QueryViewOfStreamingNode struct{}

type QueryViewOfQueryNode struct {
	NodeId     int64                   `protobuf:"varint,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Partitions []*QueryViewOfPartition `protobuf:"bytes,2,rep,name=partitions,proto3" json:"partitions,omitempty"`
}

func (m *QueryViewOfQueryNode) GetNodeId() int64 {
	if m != nil {
		return m.NodeId
	}
	return 0
}

func (m *QueryViewOfQueryNode) GetPartitions() []*QueryViewOfPartition {
	if m != nil {
		return m.Partitions
	}
	return nil
}

type QueryViewVersion struct {
	DataVersion  *DataVersion `protobuf:"bytes,1,opt,name=data_version,json=dataVersion,proto3" json:"data_version,omitempty"`
	QueryVersion int64        `protobuf:"varint,2,opt,name=query_version,json=queryVersion,proto3" json:"query_version,omitempty"`
}

func (m *QueryViewVersion) String() string {
	if m == nil {
		return "nil"
	}
	return fmt.Sprintf("%s/%d", m.GetDataVersion(), m.GetQueryVersion())
}

func (m *QueryViewVersion) GetDataVersion() *DataVersion {
	if m != nil {
		return m.DataVersion
	}
	return nil
}

func (m *QueryViewVersion) GetQueryVersion() int64 {
	if m != nil {
		return m.QueryVersion
	}
	return 0
}

type QueryViewOfPartition struct {
	PartitionId     int64   `protobuf:"varint,1,opt,name=partition_id,json=partitionId,proto3" json:"partition_id,omitempty"`
	SegmentIds      []int64 `protobuf:"varint,2,rep,packed,name=segment_ids,json=segmentIds,proto3" json:"segment_ids,omitempty"`
	ReadySegmentIds []int64 `protobuf:"varint,3,rep,packed,name=ready_segment_ids,json=readySegmentIds,proto3" json:"ready_segment_ids,omitempty"`
}

func (m *QueryViewOfPartition) GetPartitionId() int64 {
	if m != nil {
		return m.PartitionId
	}
	return 0
}

func (m *QueryViewOfPartition) GetSegmentIds() []int64 {
	if m != nil {
		return m.SegmentIds
	}
	return nil
}

func (m *QueryViewOfPartition) GetReadySegmentIds() []int64 {
	if m != nil {
		return m.ReadySegmentIds
	}
	return nil
}
