package common

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	CoordQueryViewPrefix        = "coord/qv"
	DataViewPrefix              = "datacoord-meta/dataview"
	StreamingNodeWALPrefix      = "streamingnode-meta/wal"
	StreamingNodeQueryViewDir   = "query-view"
	queryViewKeyPartCount       = 6
	dataViewKeyPartCount        = 4
	recoveryQueryViewKeyMinPart = 8
)

type QueryViewMetaEntry struct {
	Key      string
	Prefix   string
	PChannel string
	View     *QueryViewOfShard
}

type DataViewMetaEntry struct {
	Key      string
	Prefix   string
	DataView *DataViewOfCollection
}

func ListCoordQueryViews(ctx context.Context, client kv.MetaKV, metaPath string) ([]QueryViewMetaEntry, error) {
	return ListCoordQueryViewsByCollection(ctx, client, metaPath, 0)
}

func ListCoordQueryViewsByCollection(ctx context.Context, client kv.MetaKV, metaPath string, collectionID int64) ([]QueryViewMetaEntry, error) {
	basePrefix := path.Join(metaPath, CoordQueryViewPrefix) + "/"
	scanPrefix := basePrefix
	if collectionID > 0 {
		scanPrefix = path.Join(basePrefix, strconv.FormatInt(collectionID, 10)) + "/"
	}
	views, err := listViewProtoWithKeys[QueryViewOfShard](ctx, client, scanPrefix)
	if err != nil {
		return nil, err
	}
	entries := make([]QueryViewMetaEntry, 0, len(views))
	for _, view := range views {
		entries = append(entries, QueryViewMetaEntry{
			Key:    view.Key,
			Prefix: basePrefix,
			View:   view.Value,
		})
	}
	return entries, nil
}

func ListDataViews(ctx context.Context, client kv.MetaKV, metaPath string) ([]DataViewMetaEntry, error) {
	return ListDataViewsByCollection(ctx, client, metaPath, 0)
}

func ListDataViewsByCollection(ctx context.Context, client kv.MetaKV, metaPath string, collectionID int64) ([]DataViewMetaEntry, error) {
	basePrefix := path.Join(metaPath, DataViewPrefix) + "/"
	scanPrefix := basePrefix
	if collectionID > 0 {
		scanPrefix = path.Join(basePrefix, strconv.FormatInt(collectionID, 10), "versions") + "/"
	}
	views, err := listViewProtoWithKeys[DataViewOfCollection](ctx, client, scanPrefix)
	if err != nil {
		return nil, err
	}
	entries := make([]DataViewMetaEntry, 0, len(views))
	for _, view := range views {
		entries = append(entries, DataViewMetaEntry{
			Key:      view.Key,
			Prefix:   basePrefix,
			DataView: view.Value,
		})
	}
	return entries, nil
}

func ListStreamingNodeQueryViewRecovery(ctx context.Context, client kv.MetaKV, metaPath string) ([]QueryViewMetaEntry, error) {
	return ListStreamingNodeQueryViewRecoveryBy(ctx, client, metaPath, "", 0)
}

func ListStreamingNodeQueryViewRecoveryBy(ctx context.Context, client kv.MetaKV, metaPath string, pchannel string, collectionID int64) ([]QueryViewMetaEntry, error) {
	basePrefix := path.Join(metaPath, StreamingNodeWALPrefix) + "/"
	scanPrefix := basePrefix
	if pchannel != "" {
		scanPrefix = path.Join(basePrefix, pchannel, StreamingNodeQueryViewDir) + "/"
		if collectionID > 0 {
			scanPrefix = path.Join(scanPrefix, strconv.FormatInt(collectionID, 10)) + "/"
		}
	}
	views, err := listViewProtoWithKeys[QueryViewOfShard](ctx, client, scanPrefix)
	if err != nil {
		return nil, err
	}
	entries := make([]QueryViewMetaEntry, 0, len(views))
	for _, view := range views {
		pchannel, ok := RecoveryQueryViewPChannel(basePrefix, view.Key)
		if !ok {
			continue
		}
		entries = append(entries, QueryViewMetaEntry{
			Key:      view.Key,
			Prefix:   basePrefix,
			PChannel: pchannel,
			View:     view.Value,
		})
	}
	return entries, nil
}

func CoordQueryViewKeyMatches(prefix, key string, meta *QueryViewMeta) bool {
	parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
	if len(parts) != queryViewKeyPartCount || meta == nil || meta.GetVersion().GetDataVersion() == nil {
		return false
	}
	return parts[0] == strconv.FormatInt(meta.GetCollectionId(), 10) &&
		parts[1] == strconv.FormatInt(meta.GetReplicaId(), 10) &&
		parts[2] == meta.GetVchannel() &&
		parts[3] == strconv.FormatInt(meta.GetVersion().GetDataVersion().GetStreamingVersion(), 10) &&
		parts[4] == strconv.FormatInt(meta.GetVersion().GetDataVersion().GetCompactVersion(), 10) &&
		parts[5] == strconv.FormatInt(meta.GetVersion().GetQueryVersion(), 10)
}

func DataViewKeyMatches(prefix, key string, view *DataViewOfCollection) bool {
	parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
	if len(parts) != dataViewKeyPartCount || parts[1] != "versions" || view == nil || view.GetDataVersion() == nil {
		return false
	}
	return parts[0] == strconv.FormatInt(view.GetCollectionId(), 10) &&
		parts[2] == strconv.FormatInt(view.GetDataVersion().GetStreamingVersion(), 10) &&
		parts[3] == strconv.FormatInt(view.GetDataVersion().GetCompactVersion(), 10)
}

func RecoveryQueryViewKeyMatches(prefix, key string, pchannel string, meta *QueryViewMeta) bool {
	parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
	if len(parts) != recoveryQueryViewKeyMinPart || meta == nil || meta.GetVersion().GetDataVersion() == nil {
		return false
	}
	if parts[0] != pchannel || parts[1] != StreamingNodeQueryViewDir {
		return false
	}
	return parts[2] == strconv.FormatInt(meta.GetCollectionId(), 10) &&
		parts[3] == strconv.FormatInt(meta.GetReplicaId(), 10) &&
		parts[4] == meta.GetVchannel() &&
		parts[5] == strconv.FormatInt(meta.GetVersion().GetDataVersion().GetStreamingVersion(), 10) &&
		parts[6] == strconv.FormatInt(meta.GetVersion().GetDataVersion().GetCompactVersion(), 10) &&
		parts[7] == strconv.FormatInt(meta.GetVersion().GetQueryVersion(), 10)
}

func RecoveryQueryViewPChannel(prefix, key string) (string, bool) {
	parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
	if len(parts) < recoveryQueryViewKeyMinPart || parts[1] != StreamingNodeQueryViewDir {
		return "", false
	}
	return parts[0], true
}

type protoWithKey[T any] struct {
	Key   string
	Value *T
}

func listViewProtoWithKeys[T any, P interface {
	*T
	proto.Message
}](ctx context.Context, client kv.MetaKV, prefix string) ([]protoWithKey[T], error) {
	keys, vals, err := client.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("error: keys and vals of different size in qview proto list:%d vs %d", len(keys), len(vals))
	}
	result := make([]protoWithKey[T], 0, len(keys))
	for i, val := range vals {
		var elem T
		msg := P(&elem)
		if err := proto.Unmarshal([]byte(val), msg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal key=%s: %w", keys[i], err)
		}
		result = append(result, protoWithKey[T]{
			Key:   keys[i],
			Value: &elem,
		})
	}
	return result, nil
}
