package set

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

type SchemaParam struct {
	framework.ExecutionParam `use:"set schema" desc:"offline repair of an Array/StructArray capacity in RootCoord and latest StreamingNode schemas; stop metadata writers first"`
	CollectionID             int64  `name:"collectionID" default:"0" desc:"collection ID"`
	Field                    string `name:"field" default:"" desc:"exact top-level Array or StructArray field name"`
	MaxCapacity              int64  `name:"max-capacity" default:"0" desc:"new outer capacity (increase only, 1..4096)"`
	Backup                   string `name:"backup" default:"" desc:"new backup JSON file; default: schema-backup-*.json in current directory"`
}

type capacityPlan struct {
	CollectionID int64
	FieldID      int64
	Field        string
	Capacity     int64
	Members      []capacityMember
	Revision     int64
	Guards       []kv.RevisionedValue
	Prefixes     []kv.PrefixRevision
	Changes      []kv.ConditionalChange
}

func (c *ComponentSet) SchemaCommand(ctx context.Context, p *SchemaParam) error {
	if p.CollectionID <= 0 || p.Field == "" || p.MaxCapacity <= 0 || p.MaxCapacity > maxRepairCapacity {
		return fmt.Errorf("provide --collectionID, --field and --max-capacity in [1, %d]", maxRepairCapacity)
	}
	cli, ok := c.client.(kv.ConditionalKV)
	if !ok {
		return fmt.Errorf("set schema requires an etcd backend with consistent reads and conditional transactions")
	}
	plan, err := buildCapacityPlan(ctx, cli, c.basePath, p)
	if err != nil {
		return err
	}
	fmt.Printf("Collection %d, field %s (ID=%d), snapshot revision %d\n", p.CollectionID, p.Field, plan.FieldID, plan.Revision)
	for _, member := range plan.Members {
		fmt.Printf("  %s (ID=%d): max_capacity %d -> %d\n", member.Name, member.ID, member.Old, plan.Capacity)
	}
	for _, change := range plan.Changes {
		fmt.Printf("  metadata: %s (revision=%d)\n", change.Before.Key, change.Before.ModRevision)
	}
	if len(plan.Changes) == 0 {
		fmt.Println("All current schemas already have the requested capacity; no metadata written.")
		return nil
	}
	if !p.Run {
		fmt.Printf("Dry-run: %d records would change; historical SN schemas remain unchanged. Stop metadata writers before using --run.\n", len(plan.Changes))
		return nil
	}
	backup, err := saveCapacityBackup(p.Backup, plan)
	if err != nil {
		return fmt.Errorf("cannot back up schema repair: %w", err)
	}
	fmt.Printf("Backup: %s\n", backup)
	if err := cli.CommitChanges(ctx, plan.Guards, plan.Prefixes, plan.Changes); err != nil {
		return fmt.Errorf("%w; backup retained at %s", err, backup)
	}
	if err := verifyCapacityChanges(ctx, cli, plan.Changes); err != nil {
		return fmt.Errorf("transaction committed but read-back verification failed: %w; backup: %s", err, backup)
	}
	fmt.Printf("Updated and verified %d records; historical SN schemas, checkpoints and schema versions unchanged.\n", len(plan.Changes))
	return nil
}

func (p *capacityPlan) load(ctx context.Context, cli kv.ConditionalKV, prefix string, fence bool) ([]kv.RevisionedValue, error) {
	values, rev, err := cli.LoadPrefixAtRevision(ctx, prefix, p.Revision)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", prefix, err)
	}
	p.Revision = rev
	if fence {
		p.Prefixes = append(p.Prefixes, kv.PrefixRevision{Prefix: prefix, Revision: rev})
	}
	return values, nil
}

func (p *capacityPlan) addChange(before kv.RevisionedValue, old, updated proto.Message) error {
	p.Guards = append(p.Guards, before)
	if proto.Equal(old, updated) {
		return nil
	}
	data, err := proto.Marshal(updated)
	if err != nil {
		return err
	}
	p.Changes = append(p.Changes, kv.ConditionalChange{Before: before, After: data})
	return nil
}

func buildCapacityPlan(ctx context.Context, cli kv.ConditionalKV, base string, param *SchemaParam) (*capacityPlan, error) {
	p := &capacityPlan{CollectionID: param.CollectionID, Field: param.Field, Capacity: param.MaxCapacity}
	collection, record, err := loadCapacityCollection(ctx, cli, base, p)
	if err != nil {
		return nil, err
	}
	p.Guards = append(p.Guards, record)
	schema := proto.Clone(collection.Schema).(*schemapb.CollectionSchema)
	fieldRecords := make(map[int64]kv.RevisionedValue)
	for _, prefix := range []string{common.FieldMetaPrefix, common.StructArrayFieldMetaPrefix} {
		fullPrefix := path.Join(base, prefix, strconv.FormatInt(p.CollectionID, 10)) + "/"
		values, err := p.load(ctx, cli, fullPrefix, true)
		if err != nil {
			return nil, err
		}
		if len(values) > 0 && (len(collection.Schema.Fields) > 0 || len(collection.Schema.StructArrayFields) > 0) {
			return nil, fmt.Errorf("mixed embedded and split RootCoord schema layout")
		}
		for _, value := range values {
			id, err := strconv.ParseInt(strings.TrimPrefix(value.Key, fullPrefix), 10, 64)
			if err != nil || id <= 0 {
				return nil, fmt.Errorf("invalid field key %s", value.Key)
			}
			if _, exists := fieldRecords[id]; exists {
				return nil, fmt.Errorf("duplicate RootCoord field ID %d", id)
			}
			if prefix == common.FieldMetaPrefix {
				field := &schemapb.FieldSchema{}
				if err := proto.Unmarshal(value.Value, field); err != nil {
					return nil, fmt.Errorf("decode %s: %w", value.Key, err)
				}
				if field.FieldID != id {
					return nil, fmt.Errorf("field ID mismatch at %s", value.Key)
				}
				schema.Fields = append(schema.Fields, field)
			} else {
				field := &schemapb.StructArrayFieldSchema{}
				if err := proto.Unmarshal(value.Value, field); err != nil {
					return nil, fmt.Errorf("decode %s: %w", value.Key, err)
				}
				if field.FieldID != id {
					return nil, fmt.Errorf("StructArray ID mismatch at %s", value.Key)
				}
				schema.StructArrayFields = append(schema.StructArrayFields, field)
			}
			fieldRecords[id] = value
		}
	}
	updated := proto.Clone(schema).(*schemapb.CollectionSchema)
	target, err := patchSchemaCapacity(updated, p.Field, p.Capacity)
	if err != nil {
		return nil, err
	}
	p.FieldID, p.Members = target.ID, target.Members
	if value, split := fieldRecords[target.ID]; split {
		old := target.Message.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(value.Value, old); err != nil {
			return nil, err
		}
		if err := p.addRootChange(ctx, cli, base, value, old, target.Message); err != nil {
			return nil, err
		}
	} else {
		updatedCollection := proto.Clone(collection).(*etcdpb.CollectionInfo)
		updatedCollection.Schema = updated
		if err := p.addRootChange(ctx, cli, base, record, collection, updatedCollection); err != nil {
			return nil, err
		}
	}
	if err := p.addStreamingChanges(ctx, cli, base, collection, target); err != nil {
		return nil, err
	}
	return p, nil
}

func loadCapacityCollection(ctx context.Context, cli kv.ConditionalKV, base string, p *capacityPlan) (*etcdpb.CollectionInfo, kv.RevisionedValue, error) {
	var result *etcdpb.CollectionInfo
	var record kv.RevisionedValue
	for _, prefix := range []string{common.CollectionMetaPrefix, common.DBCollectionMetaPrefix} {
		fullPrefix := path.Join(base, prefix) + "/"
		values, err := p.load(ctx, cli, fullPrefix, false)
		if err != nil {
			return nil, record, err
		}
		for _, value := range values {
			if path.Base(value.Key) != strconv.FormatInt(p.CollectionID, 10) {
				continue
			}
			if result != nil {
				return nil, record, fmt.Errorf("multiple RootCoord records for collection %d", p.CollectionID)
			}
			result = &etcdpb.CollectionInfo{}
			if err := proto.Unmarshal(value.Value, result); err != nil {
				return nil, record, fmt.Errorf("decode %s: %w", value.Key, err)
			}
			record = value
		}
	}
	if result == nil || result.GetID() != p.CollectionID || result.Schema == nil {
		return nil, record, fmt.Errorf("collection %d or its schema not found", p.CollectionID)
	}
	if result.State != etcdpb.CollectionState_CollectionCreated {
		return nil, record, fmt.Errorf("collection %d is in state %s", p.CollectionID, result.State)
	}
	return result, record, nil
}

// SuffixSnapshot deployments keep a live value plus timestamped versions.
// Update only the newest matching snapshot; older snapshots stay byte-identical.
func (p *capacityPlan) addRootChange(ctx context.Context, cli kv.ConditionalKV, base string, value kv.RevisionedValue, old, updated proto.Message) error {
	if err := p.addChange(value, old, updated); err != nil {
		return err
	}
	prefix := path.Join(base, "snapshots", strings.TrimPrefix(value.Key, strings.TrimSuffix(base, "/")+"/")) + "_ts"
	values, err := p.load(ctx, cli, prefix, true)
	if err != nil {
		return err
	}
	var latest *kv.RevisionedValue
	var latestTS uint64
	for i := range values {
		ts, err := strconv.ParseUint(strings.TrimPrefix(values[i].Key, prefix), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid snapshot key %s", values[i].Key)
		}
		if latest == nil || ts > latestTS {
			latest, latestTS = &values[i], ts
		}
	}
	if latest == nil {
		return nil
	}
	snapshot := old.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(latest.Value, snapshot); err != nil {
		return fmt.Errorf("decode %s: %w", latest.Key, err)
	}
	if !proto.Equal(old, snapshot) {
		return fmt.Errorf("latest RootCoord snapshot differs from live value: %s", latest.Key)
	}
	return p.addChange(*latest, snapshot, updated)
}

func (p *capacityPlan) addStreamingChanges(ctx context.Context, cli kv.ConditionalKV, base string, collection *etcdpb.CollectionInfo, target *capacityTarget) error {
	if len(collection.VirtualChannelNames) == 0 || len(collection.VirtualChannelNames) != len(collection.PhysicalChannelNames) {
		return fmt.Errorf("collection has missing or inconsistent channel mappings")
	}
	seen := make(map[string]bool)
	for i, vchannel := range collection.VirtualChannelNames {
		pchannel := collection.PhysicalChannelNames[i]
		if vchannel == "" || pchannel == "" || vchannel == "." || vchannel == ".." || pchannel == "." || pchannel == ".." || strings.Contains(vchannel+pchannel, "/") || seen[vchannel] {
			return fmt.Errorf("invalid or duplicate vchannel mapping %q", vchannel)
		}
		seen[vchannel] = true
		key := path.Join(base, "streamingnode-meta/wal", pchannel, "vchannel", vchannel)
		values, err := p.load(ctx, cli, key, true)
		if err != nil {
			return err
		}
		var channel *streamingpb.VChannelMeta
		var latest *kv.RevisionedValue
		var latestTS uint64
		for j := range values {
			value := &values[j]
			switch {
			case value.Key == key:
				channel = &streamingpb.VChannelMeta{}
				if err := proto.Unmarshal(value.Value, channel); err != nil {
					return fmt.Errorf("decode %s: %w", key, err)
				}
				p.Guards = append(p.Guards, *value)
			case strings.HasPrefix(value.Key, key+"/schema/"):
				ts, err := strconv.ParseUint(strings.TrimPrefix(value.Key, key+"/schema/"), 10, 64)
				if err != nil {
					return fmt.Errorf("invalid schema key %s", value.Key)
				}
				if latest == nil || ts > latestTS {
					latest, latestTS = value, ts
				}
			}
		}
		if channel == nil || channel.GetVchannel() != vchannel || channel.GetCollectionInfo().GetCollectionId() != p.CollectionID || channel.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
			return fmt.Errorf("missing, mismatched or non-normal vchannel meta at %s", key)
		}
		if len(channel.GetCollectionInfo().GetSchemas()) != 0 {
			return fmt.Errorf("embedded SN schema history is not supported at %s", key)
		}
		if latest == nil {
			return fmt.Errorf("no StreamingNode schema for %s", vchannel)
		}
		if latestTS > channel.CheckpointTimeTick {
			return fmt.Errorf("latest schema %s is newer than published checkpoint %d; resolve pending schema persistence first", latest.Key, channel.CheckpointTimeTick)
		}
		old := &streamingpb.CollectionSchemaOfVChannel{}
		if err := proto.Unmarshal(latest.Value, old); err != nil {
			return fmt.Errorf("decode %s: %w", latest.Key, err)
		}
		if old.CheckpointTimeTick != latestTS || old.State != streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL || old.Schema == nil {
			return fmt.Errorf("invalid latest schema at %s", latest.Key)
		}
		if old.Schema.Version != collection.Schema.Version || old.Schema.Name != collection.Schema.Name {
			return fmt.Errorf("RootCoord and latest StreamingNode schema identity/version differ at %s", latest.Key)
		}
		updated := proto.Clone(old).(*streamingpb.CollectionSchemaOfVChannel)
		snTarget, err := patchSchemaCapacity(updated.Schema, p.Field, p.Capacity)
		if err != nil {
			return fmt.Errorf("%s: %w", latest.Key, err)
		}
		if snTarget.ID != target.ID || !slices.Equal(snTarget.Members, target.Members) || !proto.Equal(snTarget.Message, target.Message) {
			return fmt.Errorf("RootCoord and StreamingNode target fields differ at %s", latest.Key)
		}
		if err := p.addChange(*latest, old, updated); err != nil {
			return err
		}
	}
	return nil
}

func saveCapacityBackup(filename string, plan *capacityPlan) (string, error) {
	var file *os.File
	var err error
	if filename == "" {
		file, err = os.CreateTemp(".", "schema-backup-*.json")
	} else {
		file, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	}
	if err != nil {
		return "", err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(plan); err != nil {
		return file.Name(), err
	}
	if err := file.Sync(); err != nil {
		return file.Name(), err
	}
	return file.Name(), file.Close()
}

func verifyCapacityChanges(ctx context.Context, cli kv.ConditionalKV, changes []kv.ConditionalChange) error {
	var revision int64
	for _, change := range changes {
		values, rev, err := cli.LoadPrefixAtRevision(ctx, change.Before.Key, revision)
		if err != nil {
			return err
		}
		revision = rev
		found := false
		for _, value := range values {
			if value.Key == change.Before.Key {
				found = bytes.Equal(value.Value, change.After)
			}
		}
		if !found {
			return fmt.Errorf("unexpected value at %s", change.Before.Key)
		}
	}
	return nil
}
