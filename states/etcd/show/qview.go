package show

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type QueryViewParam struct {
	framework.DataSetParam `use:"show qv" desc:"preview coord-side query view meta"`
	CollectionID           int64  `name:"collection" default:"0" desc:"collection id to filter"`
	ReplicaID              int64  `name:"replica" default:"0" desc:"replica id to filter"`
	VChannel               string `name:"vchannel" default:"" desc:"vchannel to filter"`
	State                  string `name:"state" default:"" desc:"query view state to filter"`
	Version                string `name:"version" default:"" desc:"query view version to filter, format streaming/compact/query"`
	Detail                 bool   `name:"detail" default:"false" desc:"show query node partition details"`
	ShowKey                bool   `name:"show-key" default:"false" desc:"show etcd key"`
	Limit                  int64  `name:"limit" default:"0" desc:"max rows to show, 0 means unlimited"`
}

func (c *ComponentShow) QvCommand(ctx context.Context, p *QueryViewParam) (*framework.PresetResultSet, error) {
	entries, err := common.ListCoordQueryViewsByCollection(ctx, c.client, c.metaPath, p.CollectionID)
	if err != nil {
		return nil, err
	}
	entries = filterQueryViewEntries(entries, qvFilter{
		collectionID: p.CollectionID,
		replicaID:    p.ReplicaID,
		vchannel:     p.VChannel,
		state:        p.State,
		version:      p.Version,
		limit:        p.Limit,
	})
	sortQueryViewEntries(entries)
	return framework.NewPresetResultSet(&queryViewResultSet{
		title:   "QueryViews at Coord",
		entries: entries,
		detail:  p.Detail,
		showKey: p.ShowKey,
	}, framework.NameFormat(p.Format)), nil
}

type DataViewParam struct {
	framework.DataSetParam `use:"show dataview" desc:"preview datacoord data view meta"`
	CollectionID           int64  `name:"collection" default:"0" desc:"collection id to filter"`
	Version                string `name:"version" default:"" desc:"data view version to filter, format streaming/compact"`
	Detail                 bool   `name:"detail" default:"false" desc:"show shard partition details"`
	ShowKey                bool   `name:"show-key" default:"false" desc:"show etcd key"`
	Limit                  int64  `name:"limit" default:"0" desc:"max rows to show, 0 means unlimited"`
}

func (c *ComponentShow) DataviewCommand(ctx context.Context, p *DataViewParam) (*framework.PresetResultSet, error) {
	entries, err := common.ListDataViewsByCollection(ctx, c.client, c.metaPath, p.CollectionID)
	if err != nil {
		return nil, err
	}
	entries = filterDataViewEntries(entries, p.CollectionID, p.Version, p.Limit)
	sort.Slice(entries, func(i, j int) bool {
		li, lj := entries[i].DataView, entries[j].DataView
		if li.GetCollectionId() != lj.GetCollectionId() {
			return li.GetCollectionId() < lj.GetCollectionId()
		}
		return dataVersionString(li.GetDataVersion()) < dataVersionString(lj.GetDataVersion())
	})
	return framework.NewPresetResultSet(&dataViewResultSet{
		entries: entries,
		detail:  p.Detail,
		showKey: p.ShowKey,
	}, framework.NameFormat(p.Format)), nil
}

type QueryViewRecoveryParam struct {
	framework.DataSetParam `use:"show qv-recovery" desc:"preview streamingnode query view recovery meta"`
	PChannel               string `name:"pchannel" default:"" desc:"pchannel to filter"`
	CollectionID           int64  `name:"collection" default:"0" desc:"collection id to filter"`
	ReplicaID              int64  `name:"replica" default:"0" desc:"replica id to filter"`
	VChannel               string `name:"vchannel" default:"" desc:"vchannel to filter"`
	State                  string `name:"state" default:"" desc:"query view state to filter"`
	Version                string `name:"version" default:"" desc:"query view version to filter, format streaming/compact/query"`
	Detail                 bool   `name:"detail" default:"false" desc:"show query node partition details"`
	ShowKey                bool   `name:"show-key" default:"false" desc:"show etcd key"`
	Limit                  int64  `name:"limit" default:"0" desc:"max rows to show, 0 means unlimited"`
}

func (c *ComponentShow) QvRecoveryCommand(ctx context.Context, p *QueryViewRecoveryParam) (*framework.PresetResultSet, error) {
	entries, err := common.ListStreamingNodeQueryViewRecoveryBy(ctx, c.client, c.metaPath, p.PChannel, p.CollectionID)
	if err != nil {
		return nil, err
	}
	entries = filterQueryViewEntries(entries, qvFilter{
		pchannel:     p.PChannel,
		collectionID: p.CollectionID,
		replicaID:    p.ReplicaID,
		vchannel:     p.VChannel,
		state:        p.State,
		version:      p.Version,
		limit:        p.Limit,
	})
	sortQueryViewEntries(entries)
	return framework.NewPresetResultSet(&queryViewResultSet{
		title:    "QueryViews at StreamingNode Recovery",
		entries:  entries,
		detail:   p.Detail,
		showKey:  p.ShowKey,
		recovery: true,
	}, framework.NameFormat(p.Format)), nil
}

type queryViewResultSet struct {
	title    string
	entries  []common.QueryViewMetaEntry
	detail   bool
	showKey  bool
	recovery bool
}

func (rs *queryViewResultSet) Entities() any {
	rows := make([]queryViewEntity, 0, len(rs.entries))
	for _, entry := range rs.entries {
		rows = append(rows, buildQueryViewEntity(entry, rs.recovery, true))
	}
	return rows
}

func (rs *queryViewResultSet) TableTitle() string {
	return rs.title
}

func (rs *queryViewResultSet) TableHeaders() table.Row {
	header := table.Row{"Collection", "Replica", "VChannel", "State", "Version", "LoadInfoVer", "TransformFrom", "QNodes", "Segments", "ReadySegs", "HasSN"}
	if rs.recovery {
		header = append(table.Row{"PChannel"}, header...)
	}
	if rs.showKey {
		header = append(header, "Key")
	}
	return header
}

func (rs *queryViewResultSet) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(rs.entries))
	for _, entry := range rs.entries {
		e := buildQueryViewEntity(entry, rs.recovery, false)
		row := table.Row{e.CollectionID, e.ReplicaID, e.VChannel, e.State, e.Version, e.LoadInfoVersion, e.TransformStartAfterTimeTick, e.QueryNodes, e.Segments, e.ReadySegments, e.HasStreamingNode}
		if rs.recovery {
			row = append(table.Row{e.PChannel}, row...)
		}
		if rs.showKey {
			row = append(row, e.Key)
		}
		rows = append(rows, row)
	}
	return rows
}

func (rs *queryViewResultSet) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatJSON:
		return framework.MarshalJSON(rs.Entities())
	case framework.FormatLine:
		lines := make([]string, 0, len(rs.entries))
		for _, entry := range rs.entries {
			lines = append(lines, buildQueryViewEntity(entry, rs.recovery, false).Line())
		}
		return strings.Join(lines, "\n")
	case framework.FormatTable:
		return framework.RenderTable(rs.TableHeaders(), rs.TableRows(), rs.TableTitle())
	default:
		if rs.detail {
			return rs.detailString()
		}
		return framework.RenderTable(rs.TableHeaders(), rs.TableRows(), rs.TableTitle())
	}
}

func (rs *queryViewResultSet) detailString() string {
	var sb strings.Builder
	sb.WriteString(rs.title)
	sb.WriteByte('\n')
	for _, entry := range rs.entries {
		e := buildQueryViewEntity(entry, rs.recovery, false)
		if rs.recovery {
			fmt.Fprintf(&sb, "\nPChannel %s Collection %d Replica %d VChannel %s Version %s State %s\n",
				e.PChannel, e.CollectionID, e.ReplicaID, e.VChannel, e.Version, e.State)
		} else {
			fmt.Fprintf(&sb, "\nCollection %d Replica %d VChannel %s Version %s State %s\n",
				e.CollectionID, e.ReplicaID, e.VChannel, e.Version, e.State)
		}
		fmt.Fprintf(&sb, "LoadInfoVersion=%d TransformStartAfterTimeTick=%d StreamingNode=%t\n",
			e.LoadInfoVersion, e.TransformStartAfterTimeTick, e.HasStreamingNode)
		if rs.showKey {
			fmt.Fprintf(&sb, "Key=%s\n", e.Key)
		}
		for _, qn := range entry.View.GetQueryNode() {
			fmt.Fprintf(&sb, "  QueryNode %d\n", qn.GetNodeId())
			for _, part := range qn.GetPartitions() {
				fmt.Fprintf(&sb, "    Partition %d: segments=%d ready=%d ids=%s\n",
					part.GetPartitionId(), len(part.GetSegmentIds()), len(part.GetReadySegmentIds()), compactInt64List(part.GetSegmentIds(), 32))
			}
		}
	}
	return strings.TrimRight(sb.String(), "\n")
}

type dataViewResultSet struct {
	entries []common.DataViewMetaEntry
	detail  bool
	showKey bool
}

func (rs *dataViewResultSet) Entities() any {
	rows := make([]dataViewEntity, 0, len(rs.entries))
	for _, entry := range rs.entries {
		rows = append(rows, buildDataViewEntity(entry, true))
	}
	return rows
}

func (rs *dataViewResultSet) TableTitle() string {
	return "DataViews at DataCoord"
}

func (rs *dataViewResultSet) TableHeaders() table.Row {
	header := table.Row{"Collection", "Version", "Shards", "Partitions", "Segments", "TransformFromMin", "TransformFromMax"}
	if rs.showKey {
		header = append(header, "Key")
	}
	return header
}

func (rs *dataViewResultSet) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(rs.entries))
	for _, entry := range rs.entries {
		e := buildDataViewEntity(entry, false)
		row := table.Row{e.CollectionID, e.Version, e.Shards, e.Partitions, e.Segments, e.TransformFromMin, e.TransformFromMax}
		if rs.showKey {
			row = append(row, e.Key)
		}
		rows = append(rows, row)
	}
	return rows
}

func (rs *dataViewResultSet) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatJSON:
		return framework.MarshalJSON(rs.Entities())
	case framework.FormatLine:
		lines := make([]string, 0, len(rs.entries))
		for _, entry := range rs.entries {
			lines = append(lines, buildDataViewEntity(entry, false).Line())
		}
		return strings.Join(lines, "\n")
	case framework.FormatTable:
		return framework.RenderTable(rs.TableHeaders(), rs.TableRows(), rs.TableTitle())
	default:
		if rs.detail {
			return rs.detailString()
		}
		return framework.RenderTable(rs.TableHeaders(), rs.TableRows(), rs.TableTitle())
	}
}

func (rs *dataViewResultSet) detailString() string {
	var sb strings.Builder
	sb.WriteString("DataViews at DataCoord")
	sb.WriteByte('\n')
	for _, entry := range rs.entries {
		e := buildDataViewEntity(entry, false)
		fmt.Fprintf(&sb, "\nCollection %d Version %s Shards=%d Partitions=%d Segments=%d\n",
			e.CollectionID, e.Version, e.Shards, e.Partitions, e.Segments)
		if rs.showKey {
			fmt.Fprintf(&sb, "Key=%s\n", e.Key)
		}
		for _, shard := range entry.DataView.GetShards() {
			fmt.Fprintf(&sb, "  Shard %s transform_from=%d partitions=%d segments=%d\n",
				shard.GetVchannel(), shard.GetTransformStartAfterTimetick(), len(shard.GetPartitions()), dataViewShardSegmentCount(shard))
			for _, part := range shard.GetPartitions() {
				fmt.Fprintf(&sb, "    Partition %d: segments=%d ids=%s\n",
					part.GetPartitionId(), len(part.GetSegmentIds()), compactInt64List(part.GetSegmentIds(), 32))
			}
		}
	}
	return strings.TrimRight(sb.String(), "\n")
}

type qvFilter struct {
	pchannel     string
	collectionID int64
	replicaID    int64
	vchannel     string
	state        string
	version      string
	limit        int64
}

func filterQueryViewEntries(entries []common.QueryViewMetaEntry, filter qvFilter) []common.QueryViewMetaEntry {
	result := make([]common.QueryViewMetaEntry, 0, len(entries))
	for _, entry := range entries {
		meta := entry.View.GetMeta()
		if filter.pchannel != "" && entry.PChannel != filter.pchannel {
			continue
		}
		if filter.collectionID > 0 && meta.GetCollectionId() != filter.collectionID {
			continue
		}
		if filter.replicaID > 0 && meta.GetReplicaId() != filter.replicaID {
			continue
		}
		if filter.vchannel != "" && meta.GetVchannel() != filter.vchannel {
			continue
		}
		if filter.state != "" && !strings.EqualFold(queryViewStateString(meta.GetState()), filter.state) {
			continue
		}
		if filter.version != "" && queryViewVersionString(meta.GetVersion()) != filter.version {
			continue
		}
		result = append(result, entry)
		if filter.limit > 0 && int64(len(result)) >= filter.limit {
			break
		}
	}
	return result
}

func filterDataViewEntries(entries []common.DataViewMetaEntry, collectionID int64, version string, limit int64) []common.DataViewMetaEntry {
	result := make([]common.DataViewMetaEntry, 0, len(entries))
	for _, entry := range entries {
		view := entry.DataView
		if collectionID > 0 && view.GetCollectionId() != collectionID {
			continue
		}
		if version != "" && dataVersionString(view.GetDataVersion()) != version {
			continue
		}
		result = append(result, entry)
		if limit > 0 && int64(len(result)) >= limit {
			break
		}
	}
	return result
}

func sortQueryViewEntries(entries []common.QueryViewMetaEntry) {
	sort.Slice(entries, func(i, j int) bool {
		li, lj := buildQueryViewEntity(entries[i], false, false), buildQueryViewEntity(entries[j], false, false)
		if li.CollectionID != lj.CollectionID {
			return li.CollectionID < lj.CollectionID
		}
		if li.ReplicaID != lj.ReplicaID {
			return li.ReplicaID < lj.ReplicaID
		}
		if li.VChannel != lj.VChannel {
			return li.VChannel < lj.VChannel
		}
		return li.Version < lj.Version
	})
}

type queryViewEntity struct {
	Key                         string                    `json:"key,omitempty"`
	PChannel                    string                    `json:"pchannel,omitempty"`
	CollectionID                int64                     `json:"collection_id"`
	ReplicaID                   int64                     `json:"replica_id"`
	VChannel                    string                    `json:"vchannel"`
	State                       string                    `json:"state"`
	Version                     string                    `json:"version"`
	LoadInfoVersion             uint64                    `json:"load_info_version"`
	TransformStartAfterTimeTick uint64                    `json:"transform_start_after_timetick"`
	QueryNodes                  int                       `json:"query_nodes"`
	Segments                    int                       `json:"segments"`
	ReadySegments               int                       `json:"ready_segments"`
	HasStreamingNode            bool                      `json:"has_streaming_node"`
	QueryNodeDetails            []queryNodeAssignmentInfo `json:"query_node_details,omitempty"`
}

type queryNodeAssignmentInfo struct {
	NodeID     int64                     `json:"node_id"`
	Partitions []partitionAssignmentInfo `json:"partitions"`
}

type partitionAssignmentInfo struct {
	PartitionID     int64   `json:"partition_id"`
	SegmentIDs      []int64 `json:"segment_ids"`
	ReadySegmentIDs []int64 `json:"ready_segment_ids,omitempty"`
}

func (e queryViewEntity) Line() string {
	if e.PChannel != "" {
		return fmt.Sprintf("pchannel=%s collection=%d replica=%d vchannel=%s state=%s version=%s qnodes=%d segments=%d",
			e.PChannel, e.CollectionID, e.ReplicaID, e.VChannel, e.State, e.Version, e.QueryNodes, e.Segments)
	}
	return fmt.Sprintf("collection=%d replica=%d vchannel=%s state=%s version=%s qnodes=%d segments=%d",
		e.CollectionID, e.ReplicaID, e.VChannel, e.State, e.Version, e.QueryNodes, e.Segments)
}

func buildQueryViewEntity(entry common.QueryViewMetaEntry, recovery bool, withDetail bool) queryViewEntity {
	view := entry.View
	meta := view.GetMeta()
	segments, ready := queryViewSegmentCounts(view)
	e := queryViewEntity{
		Key:                         entry.Key,
		CollectionID:                meta.GetCollectionId(),
		ReplicaID:                   meta.GetReplicaId(),
		VChannel:                    meta.GetVchannel(),
		State:                       queryViewStateString(meta.GetState()),
		Version:                     queryViewVersionString(meta.GetVersion()),
		LoadInfoVersion:             meta.GetLoadInfoVersion(),
		TransformStartAfterTimeTick: meta.GetTransformStartAfterTimetick(),
		QueryNodes:                  len(view.GetQueryNode()),
		Segments:                    segments,
		ReadySegments:               ready,
		HasStreamingNode:            view.GetStreamingNode() != nil,
	}
	if recovery {
		e.PChannel = entry.PChannel
	}
	if withDetail {
		e.QueryNodeDetails = make([]queryNodeAssignmentInfo, 0, len(view.GetQueryNode()))
		for _, qn := range view.GetQueryNode() {
			info := queryNodeAssignmentInfo{
				NodeID:     qn.GetNodeId(),
				Partitions: make([]partitionAssignmentInfo, 0, len(qn.GetPartitions())),
			}
			for _, part := range qn.GetPartitions() {
				info.Partitions = append(info.Partitions, partitionAssignmentInfo{
					PartitionID:     part.GetPartitionId(),
					SegmentIDs:      append([]int64(nil), part.GetSegmentIds()...),
					ReadySegmentIDs: append([]int64(nil), part.GetReadySegmentIds()...),
				})
			}
			e.QueryNodeDetails = append(e.QueryNodeDetails, info)
		}
	}
	return e
}

type dataViewEntity struct {
	Key              string                `json:"key,omitempty"`
	CollectionID     int64                 `json:"collection_id"`
	Version          string                `json:"version"`
	Shards           int                   `json:"shards"`
	Partitions       int                   `json:"partitions"`
	Segments         int                   `json:"segments"`
	TransformFromMin uint64                `json:"transform_from_min"`
	TransformFromMax uint64                `json:"transform_from_max"`
	ShardDetails     []dataViewShardEntity `json:"shard_details,omitempty"`
}

type dataViewShardEntity struct {
	VChannel      string                    `json:"vchannel"`
	TransformFrom uint64                    `json:"transform_start_after_timetick"`
	Partitions    []dataViewPartitionEntity `json:"partitions"`
}

type dataViewPartitionEntity struct {
	PartitionID int64   `json:"partition_id"`
	SegmentIDs  []int64 `json:"segment_ids"`
}

func (e dataViewEntity) Line() string {
	return fmt.Sprintf("collection=%d version=%s shards=%d partitions=%d segments=%d transform_from=%d..%d",
		e.CollectionID, e.Version, e.Shards, e.Partitions, e.Segments, e.TransformFromMin, e.TransformFromMax)
}

func buildDataViewEntity(entry common.DataViewMetaEntry, withDetail bool) dataViewEntity {
	view := entry.DataView
	e := dataViewEntity{
		Key:          entry.Key,
		CollectionID: view.GetCollectionId(),
		Version:      dataVersionString(view.GetDataVersion()),
		Shards:       len(view.GetShards()),
	}
	firstTick := true
	for _, shard := range view.GetShards() {
		if firstTick {
			e.TransformFromMin = shard.GetTransformStartAfterTimetick()
			e.TransformFromMax = shard.GetTransformStartAfterTimetick()
			firstTick = false
		} else {
			if shard.GetTransformStartAfterTimetick() < e.TransformFromMin {
				e.TransformFromMin = shard.GetTransformStartAfterTimetick()
			}
			if shard.GetTransformStartAfterTimetick() > e.TransformFromMax {
				e.TransformFromMax = shard.GetTransformStartAfterTimetick()
			}
		}
		e.Partitions += len(shard.GetPartitions())
		e.Segments += dataViewShardSegmentCount(shard)
		if withDetail {
			shardInfo := dataViewShardEntity{
				VChannel:      shard.GetVchannel(),
				TransformFrom: shard.GetTransformStartAfterTimetick(),
				Partitions:    make([]dataViewPartitionEntity, 0, len(shard.GetPartitions())),
			}
			for _, part := range shard.GetPartitions() {
				shardInfo.Partitions = append(shardInfo.Partitions, dataViewPartitionEntity{
					PartitionID: part.GetPartitionId(),
					SegmentIDs:  append([]int64(nil), part.GetSegmentIds()...),
				})
			}
			e.ShardDetails = append(e.ShardDetails, shardInfo)
		}
	}
	return e
}

func queryViewSegmentCounts(view *common.QueryViewOfShard) (segments int, ready int) {
	for _, qn := range view.GetQueryNode() {
		for _, part := range qn.GetPartitions() {
			segments += len(part.GetSegmentIds())
			ready += len(part.GetReadySegmentIds())
		}
	}
	return segments, ready
}

func dataViewShardSegmentCount(shard *common.DataViewOfShard) int {
	total := 0
	for _, part := range shard.GetPartitions() {
		total += len(part.GetSegmentIds())
	}
	return total
}

func dataVersionString(version *common.DataVersion) string {
	if version == nil {
		return "nil"
	}
	return fmt.Sprintf("%d/%d", version.GetStreamingVersion(), version.GetCompactVersion())
}

func queryViewVersionString(version *common.QueryViewVersion) string {
	if version == nil {
		return "nil"
	}
	return fmt.Sprintf("%s/%d", dataVersionString(version.GetDataVersion()), version.GetQueryVersion())
}

func queryViewStateString(state common.QueryViewState) string {
	return strings.TrimPrefix(state.String(), "QueryViewState")
}

func compactInt64List(values []int64, limit int) string {
	if len(values) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(values))
	for i, value := range values {
		if limit > 0 && i >= limit {
			parts = append(parts, fmt.Sprintf("...(+%d)", len(values)-limit))
			break
		}
		parts = append(parts, strconv.FormatInt(value, 10))
	}
	return "[" + strings.Join(parts, ",") + "]"
}
