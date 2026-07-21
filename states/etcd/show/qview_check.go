package show

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

const (
	qvCheckLevelError = "ERROR"
	qvCheckLevelWarn  = "WARN"
)

type QueryViewCheckParam struct {
	framework.DataSetParam `use:"show qv-check" desc:"check qv/dataview/recovery meta consistency"`
	PChannel               string `name:"pchannel" default:"" desc:"pchannel to filter"`
	CollectionID           int64  `name:"collection" default:"0" desc:"collection id to filter"`
	ReplicaID              int64  `name:"replica" default:"0" desc:"replica id to filter"`
	VChannel               string `name:"vchannel" default:"" desc:"vchannel to filter"`
	Level                  string `name:"level" default:"" desc:"issue level to filter, ERROR or WARN"`
	Type                   string `name:"type" default:"" desc:"issue type to filter"`
	Detail                 bool   `name:"detail" default:"false" desc:"show related keys and segments"`
	ShowKey                bool   `name:"show-key" default:"false" desc:"show primary etcd key"`
	Limit                  int64  `name:"limit" default:"0" desc:"max rows to show, 0 means unlimited"`
}

func (c *ComponentShow) QvCheckCommand(ctx context.Context, p *QueryViewCheckParam) (*framework.PresetResultSet, error) {
	coordViews, err := common.ListCoordQueryViews(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, err
	}
	dataViews, err := common.ListDataViews(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, err
	}
	recoveryViews, err := common.ListStreamingNodeQueryViewRecovery(ctx, c.client, c.metaPath)
	if err != nil {
		return nil, err
	}

	issues := buildQueryViewCheckIssues(coordViews, dataViews, recoveryViews)
	issues = filterQueryViewCheckIssues(issues, p)
	sortQueryViewCheckIssues(issues)
	return framework.NewPresetResultSet(&queryViewCheckResultSet{
		issues:  issues,
		detail:  p.Detail,
		showKey: p.ShowKey,
	}, framework.NameFormat(p.Format)), nil
}

type queryViewCheckIssue struct {
	Level       string   `json:"level"`
	Type        string   `json:"type"`
	Collection  int64    `json:"collection_id,omitempty"`
	Replica     int64    `json:"replica_id,omitempty"`
	VChannel    string   `json:"vchannel,omitempty"`
	PChannel    string   `json:"pchannel,omitempty"`
	Version     string   `json:"version,omitempty"`
	Message     string   `json:"message"`
	Key         string   `json:"key,omitempty"`
	RelatedKeys []string `json:"related_keys,omitempty"`
	SegmentIDs  []int64  `json:"segment_ids,omitempty"`
}

type queryViewCheckResultSet struct {
	issues  []queryViewCheckIssue
	detail  bool
	showKey bool
}

func (rs *queryViewCheckResultSet) Entities() any {
	return rs.issues
}

func (rs *queryViewCheckResultSet) TableTitle() string {
	return "QueryView Meta Check"
}

func (rs *queryViewCheckResultSet) TableHeaders() table.Row {
	header := table.Row{"Level", "Type", "Collection", "Replica", "VChannel", "PChannel", "Version", "Message"}
	if rs.showKey {
		header = append(header, "Key")
	}
	return header
}

func (rs *queryViewCheckResultSet) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(rs.issues))
	for _, issue := range rs.issues {
		row := table.Row{issue.Level, issue.Type, issue.Collection, issue.Replica, issue.VChannel, issue.PChannel, issue.Version, issue.Message}
		if rs.showKey {
			row = append(row, issue.Key)
		}
		rows = append(rows, row)
	}
	return rows
}

func (rs *queryViewCheckResultSet) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatJSON:
		return framework.MarshalJSON(rs.Entities())
	case framework.FormatLine:
		lines := make([]string, 0, len(rs.issues))
		for _, issue := range rs.issues {
			lines = append(lines, issue.Line())
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

func (rs *queryViewCheckResultSet) detailString() string {
	var sb strings.Builder
	sb.WriteString(rs.TableTitle())
	sb.WriteByte('\n')
	for _, issue := range rs.issues {
		fmt.Fprintf(&sb, "\n%s %s collection=%d replica=%d vchannel=%s pchannel=%s version=%s\n",
			issue.Level, issue.Type, issue.Collection, issue.Replica, issue.VChannel, issue.PChannel, issue.Version)
		fmt.Fprintf(&sb, "  %s\n", issue.Message)
		if issue.Key != "" {
			fmt.Fprintf(&sb, "  Key: %s\n", issue.Key)
		}
		for _, key := range issue.RelatedKeys {
			fmt.Fprintf(&sb, "  RelatedKey: %s\n", key)
		}
		if len(issue.SegmentIDs) > 0 {
			fmt.Fprintf(&sb, "  Segments: %s\n", compactInt64List(issue.SegmentIDs, 64))
		}
	}
	return strings.TrimRight(sb.String(), "\n")
}

func (issue queryViewCheckIssue) Line() string {
	return fmt.Sprintf("level=%s type=%s collection=%d replica=%d vchannel=%s pchannel=%s version=%s message=%q",
		issue.Level, issue.Type, issue.Collection, issue.Replica, issue.VChannel, issue.PChannel, issue.Version, issue.Message)
}

func buildQueryViewCheckIssues(coordViews []common.QueryViewMetaEntry, dataViews []common.DataViewMetaEntry, recoveryViews []common.QueryViewMetaEntry) []queryViewCheckIssue {
	issues := make([]queryViewCheckIssue, 0)
	dataByVersion := make(map[dataViewIdentity]common.DataViewMetaEntry, len(dataViews))
	coordUpByIdentity := make(map[queryViewIdentity]common.QueryViewMetaEntry, len(coordViews))
	recoveryByIdentity := make(map[queryViewIdentity]common.QueryViewMetaEntry, len(recoveryViews))

	for _, dataEntry := range dataViews {
		view := dataEntry.DataView
		if view == nil || view.GetDataVersion() == nil {
			issues = append(issues, queryViewCheckIssue{
				Level:   qvCheckLevelError,
				Type:    "InvalidDataViewMeta",
				Message: "dataview proto has nil data version",
				Key:     dataEntry.Key,
			})
			continue
		}
		if !common.DataViewKeyMatches(dataEntry.Prefix, dataEntry.Key, view) {
			issues = append(issues, queryViewCheckIssue{
				Level:      qvCheckLevelError,
				Type:       "DataViewKeyMetaMismatch",
				Collection: view.GetCollectionId(),
				Version:    dataVersionString(view.GetDataVersion()),
				Message:    "dataview etcd key does not match proto metadata",
				Key:        dataEntry.Key,
			})
		}
		dataByVersion[dataViewIdentityOf(view)] = dataEntry
	}

	for _, recoveryEntry := range recoveryViews {
		meta := queryViewMeta(recoveryEntry)
		if meta == nil || meta.GetVersion().GetDataVersion() == nil {
			issues = append(issues, invalidQueryViewIssue("InvalidRecoveryQueryViewMeta", recoveryEntry, "recovery query view proto has nil meta or data version"))
			continue
		}
		if !common.RecoveryQueryViewKeyMatches(recoveryEntry.Prefix, recoveryEntry.Key, recoveryEntry.PChannel, meta) {
			issues = append(issues, queryViewIssue(qvCheckLevelError, "RecoveryQueryViewKeyMetaMismatch", recoveryEntry, "recovery query view etcd key does not match proto metadata"))
		}
		recoveryByIdentity[queryViewIdentityOf(meta)] = recoveryEntry
	}

	for _, coordEntry := range coordViews {
		meta := queryViewMeta(coordEntry)
		if meta == nil || meta.GetVersion().GetDataVersion() == nil {
			issues = append(issues, invalidQueryViewIssue("InvalidCoordQueryViewMeta", coordEntry, "coord query view proto has nil meta or data version"))
			continue
		}
		if !common.CoordQueryViewKeyMatches(coordEntry.Prefix, coordEntry.Key, meta) {
			issues = append(issues, queryViewIssue(qvCheckLevelError, "CoordQueryViewKeyMetaMismatch", coordEntry, "coord query view etcd key does not match proto metadata"))
		}
		if meta.GetState() == common.QueryViewStateUp {
			coordUpByIdentity[queryViewIdentityOf(meta)] = coordEntry
		}

		dataEntry, ok := dataByVersion[dataViewIdentityOfQueryView(meta)]
		if !ok {
			issues = append(issues, queryViewIssue(qvCheckLevelError, "DataViewMissingForQueryView", coordEntry, "matching dataview version is missing"))
			continue
		}
		shard := findDataViewShard(dataEntry.DataView, meta.GetVchannel())
		if shard == nil {
			issue := queryViewIssue(qvCheckLevelError, "QueryViewVChannelMissingInDataView", coordEntry, "query view vchannel is missing from matching dataview")
			issue.RelatedKeys = []string{dataEntry.Key}
			issues = append(issues, issue)
			continue
		}
		issues = append(issues, checkQueryViewSegments(coordEntry, dataEntry, shard)...)
	}

	for identity, coordEntry := range coordUpByIdentity {
		if _, ok := recoveryByIdentity[identity]; !ok {
			issues = append(issues, queryViewIssue(qvCheckLevelWarn, "CoordUpQueryViewMissingRecovery", coordEntry, "coord Up query view has no matching streamingnode recovery meta"))
		}
	}
	for identity, recoveryEntry := range recoveryByIdentity {
		if _, ok := coordUpByIdentity[identity]; !ok {
			issues = append(issues, queryViewIssue(qvCheckLevelWarn, "StaleRecoveryQueryView", recoveryEntry, "streamingnode recovery meta has no matching coord Up query view"))
		}
	}

	return issues
}

func checkQueryViewSegments(coordEntry common.QueryViewMetaEntry, dataEntry common.DataViewMetaEntry, shard *common.DataViewOfShard) []queryViewCheckIssue {
	qvSegments := queryViewSegmentOccurrences(coordEntry.View)
	dataSegments := dataViewShardSegmentSet(shard)
	issues := make([]queryViewCheckIssue, 0)

	duplicates := make([]int64, 0)
	for segmentID, count := range qvSegments {
		if count > 1 {
			duplicates = append(duplicates, segmentID)
		}
	}
	if len(duplicates) > 0 {
		sortInt64s(duplicates)
		issue := queryViewIssue(qvCheckLevelError, "DuplicateSegmentAssignment", coordEntry, "same segment is assigned more than once in one query view")
		issue.RelatedKeys = []string{dataEntry.Key}
		issue.SegmentIDs = duplicates
		issues = append(issues, issue)
	}

	extra := make([]int64, 0)
	for segmentID := range qvSegments {
		if _, ok := dataSegments[segmentID]; !ok {
			extra = append(extra, segmentID)
		}
	}
	if len(extra) > 0 {
		sortInt64s(extra)
		issue := queryViewIssue(qvCheckLevelError, "QueryViewSegmentNotInDataView", coordEntry, "query view assigns segment ids that are absent from matching dataview shard")
		issue.RelatedKeys = []string{dataEntry.Key}
		issue.SegmentIDs = extra
		issues = append(issues, issue)
	}

	missing := make([]int64, 0)
	for segmentID := range dataSegments {
		if _, ok := qvSegments[segmentID]; !ok {
			missing = append(missing, segmentID)
		}
	}
	if len(missing) > 0 {
		sortInt64s(missing)
		issue := queryViewIssue(qvCheckLevelWarn, "DataViewSegmentNotAssignedToQueryView", coordEntry, "matching dataview shard has segment ids not assigned by query view")
		issue.RelatedKeys = []string{dataEntry.Key}
		issue.SegmentIDs = missing
		issues = append(issues, issue)
	}
	return issues
}

type queryViewIdentity struct {
	collection int64
	replica    int64
	vchannel   string
	version    string
}

type dataViewIdentity struct {
	collection int64
	version    string
}

func queryViewMeta(entry common.QueryViewMetaEntry) *common.QueryViewMeta {
	if entry.View == nil {
		return nil
	}
	return entry.View.GetMeta()
}

func queryViewIdentityOf(meta *common.QueryViewMeta) queryViewIdentity {
	return queryViewIdentity{
		collection: meta.GetCollectionId(),
		replica:    meta.GetReplicaId(),
		vchannel:   meta.GetVchannel(),
		version:    queryViewVersionString(meta.GetVersion()),
	}
}

func dataViewIdentityOf(view *common.DataViewOfCollection) dataViewIdentity {
	return dataViewIdentity{
		collection: view.GetCollectionId(),
		version:    dataVersionString(view.GetDataVersion()),
	}
}

func dataViewIdentityOfQueryView(meta *common.QueryViewMeta) dataViewIdentity {
	return dataViewIdentity{
		collection: meta.GetCollectionId(),
		version:    dataVersionString(meta.GetVersion().GetDataVersion()),
	}
}

func queryViewIssue(level string, issueType string, entry common.QueryViewMetaEntry, message string) queryViewCheckIssue {
	meta := queryViewMeta(entry)
	issue := queryViewCheckIssue{
		Level:    level,
		Type:     issueType,
		PChannel: entry.PChannel,
		Message:  message,
		Key:      entry.Key,
	}
	if meta != nil {
		issue.Collection = meta.GetCollectionId()
		issue.Replica = meta.GetReplicaId()
		issue.VChannel = meta.GetVchannel()
		issue.Version = queryViewVersionString(meta.GetVersion())
	}
	return issue
}

func invalidQueryViewIssue(issueType string, entry common.QueryViewMetaEntry, message string) queryViewCheckIssue {
	return queryViewIssue(qvCheckLevelError, issueType, entry, message)
}

func findDataViewShard(view *common.DataViewOfCollection, vchannel string) *common.DataViewOfShard {
	if view == nil {
		return nil
	}
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard
		}
	}
	return nil
}

func queryViewSegmentOccurrences(view *common.QueryViewOfShard) map[int64]int {
	segments := make(map[int64]int)
	for _, qn := range view.GetQueryNode() {
		for _, part := range qn.GetPartitions() {
			for _, segmentID := range part.GetSegmentIds() {
				segments[segmentID]++
			}
		}
	}
	return segments
}

func dataViewShardSegmentSet(shard *common.DataViewOfShard) map[int64]struct{} {
	segments := make(map[int64]struct{})
	for _, part := range shard.GetPartitions() {
		for _, segmentID := range part.GetSegmentIds() {
			segments[segmentID] = struct{}{}
		}
	}
	return segments
}

func filterQueryViewCheckIssues(issues []queryViewCheckIssue, p *QueryViewCheckParam) []queryViewCheckIssue {
	result := make([]queryViewCheckIssue, 0, len(issues))
	for _, issue := range issues {
		if p.PChannel != "" && issue.PChannel != p.PChannel {
			continue
		}
		if p.CollectionID > 0 && issue.Collection != p.CollectionID {
			continue
		}
		if p.ReplicaID > 0 && issue.Replica != p.ReplicaID {
			continue
		}
		if p.VChannel != "" && issue.VChannel != p.VChannel {
			continue
		}
		if p.Level != "" && !strings.EqualFold(issue.Level, p.Level) {
			continue
		}
		if p.Type != "" && !strings.EqualFold(issue.Type, p.Type) {
			continue
		}
		result = append(result, issue)
		if p.Limit > 0 && int64(len(result)) >= p.Limit {
			break
		}
	}
	return result
}

func sortQueryViewCheckIssues(issues []queryViewCheckIssue) {
	sort.Slice(issues, func(i, j int) bool {
		li, lj := issues[i], issues[j]
		if li.Level != lj.Level {
			return qvCheckLevelRank(li.Level) < qvCheckLevelRank(lj.Level)
		}
		if li.Collection != lj.Collection {
			return li.Collection < lj.Collection
		}
		if li.Replica != lj.Replica {
			return li.Replica < lj.Replica
		}
		if li.VChannel != lj.VChannel {
			return li.VChannel < lj.VChannel
		}
		if li.Version != lj.Version {
			return li.Version < lj.Version
		}
		return li.Type < lj.Type
	})
}

func qvCheckLevelRank(level string) int {
	switch level {
	case qvCheckLevelError:
		return 0
	case qvCheckLevelWarn:
		return 1
	default:
		return 2
	}
}

func sortInt64s(values []int64) {
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
}
