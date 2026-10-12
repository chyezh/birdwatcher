package set

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	_ "github.com/milvus-io/birdwatcher/asap"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func capacityParam(value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: "max_capacity", Value: value}
}

func lawyerSchema() *schemapb.CollectionSchema {
	structField := &schemapb.StructArrayFieldSchema{FieldID: 131, Name: "lawyer"}
	// StructArray.nullable (#6) is unknown to v2 bindings. Preserve it exactly.
	structField.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 6, protowire.VarintType), 1))
	for i, name := range []string{"party_name", "lawyer_name", "law_firm", "party_role"} {
		maxLength := "1024"
		if i == 3 {
			maxLength = "255"
		}
		member := &schemapb.FieldSchema{
			FieldID: int64(132 + i), Name: "lawyer[" + name + "]", DataType: schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar, Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: maxLength}, capacityParam("1000")},
		}
		member.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 77, protowire.BytesType), []byte("future field attribute")))
		member.TypeParams[1].ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 42))
		structField.Fields = append(structField.Fields, member)
	}
	schema := &schemapb.CollectionSchema{
		Name: "cases", Version: 7, DbName: "default",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, TypeParams: []*commonpb.KeyValuePair{capacityParam("1000")}},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{structField},
	}
	schema.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 19))
	return schema
}

func TestSchemaCapacityLawyer(t *testing.T) {
	before := lawyerSchema()
	updated := proto.Clone(before).(*schemapb.CollectionSchema)
	target, err := patchSchemaCapacity(updated, "lawyer", 4096)
	require.NoError(t, err)
	require.EqualValues(t, 131, target.ID)
	require.Len(t, target.Members, 4)
	for i, member := range updated.StructArrayFields[0].Fields {
		require.EqualValues(t, 132+i, member.FieldID)
		require.Equal(t, "4096", member.TypeParams[1].Value)
		member.TypeParams[1].Value = "1000"
	}
	// This checks every other attribute, including nullable, max_length,
	// unknown fields on members/parameters, schema version and field ordering.
	require.True(t, proto.Equal(before, updated))
}

func TestSchemaCapacityValidation(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		value  int64
		mutate func(*schemapb.CollectionSchema)
		want   string
	}{
		{name: "zero", field: "lawyer", value: 0, want: "[1, 4096]"},
		{name: "too large", field: "lawyer", value: 4097, want: "[1, 4096]"},
		{name: "decrease", field: "lawyer", value: 999, want: "cannot reduce"},
		{name: "scalar", field: "id", value: 4096, want: "not an Array"},
		{name: "child path", field: "lawyer.party_name", value: 4096, want: "parent StructArray"},
		{name: "missing parameter", field: "lawyer", value: 4096, mutate: func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[0].TypeParams = nil }, want: "missing max_capacity"},
		{name: "duplicate parameter", field: "lawyer", value: 4096, mutate: func(s *schemapb.CollectionSchema) {
			f := s.StructArrayFields[0].Fields[0]
			f.TypeParams = append(f.TypeParams, capacityParam("1000"))
		}, want: "duplicate max_capacity"},
		{name: "different capacities", field: "lawyer", value: 4096, mutate: func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[0].TypeParams[1].Value = "2000" }, want: "inconsistent member"},
		{name: "duplicate member ID", field: "lawyer", value: 4096, mutate: func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[0].FieldID = 133 }, want: "duplicate member field ID"},
		{name: "different parent capacity", field: "lawyer", value: 4096, mutate: func(s *schemapb.CollectionSchema) {
			s.StructArrayFields[0].TypeParams = []*commonpb.KeyValuePair{capacityParam("2000")}
		}, want: "differs from its members"},
		{name: "ambiguous name", field: "lawyer", value: 4096, mutate: func(s *schemapb.CollectionSchema) { s.Fields[1].Name = "lawyer" }, want: "ambiguous"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := lawyerSchema()
			if tt.mutate != nil {
				tt.mutate(schema)
			}
			_, err := patchSchemaCapacity(schema, tt.field, tt.value)
			require.ErrorContains(t, err, tt.want)
		})
	}
	t.Run("same capacity", func(t *testing.T) {
		before := lawyerSchema()
		after := proto.Clone(before).(*schemapb.CollectionSchema)
		_, err := patchSchemaCapacity(after, "lawyer", 1000)
		require.NoError(t, err)
		require.True(t, proto.Equal(before, after))
	})
}

func TestSchemaCapacityNested(t *testing.T) {
	field, outer := nestedCapacityFixture(t)
	old, err := patchArrayCapacity(field, 4096)
	require.NoError(t, err)
	require.EqualValues(t, 1000, old)
	require.Equal(t, "4096", field.TypeParams[0].Value)
	raw := field.ProtoReflect().GetUnknown()
	_, _, tagSize := protowire.ConsumeTag(raw)
	payload, _ := protowire.ConsumeBytes(raw[tagSize:])
	got := dynamicpb.NewMessage(capacityTypeDescriptor())
	want := dynamicpb.NewMessage(capacityTypeDescriptor())
	require.NoError(t, proto.Unmarshal(payload, got))
	require.NoError(t, proto.Unmarshal(outer, want))
	params := got.Get(got.Descriptor().Fields().ByNumber(3)).List()
	entry := params.Get(0).Message()
	valueField := entry.Descriptor().Fields().ByNumber(2)
	require.Equal(t, "4096", entry.Get(valueField).String())
	entry.Set(valueField, protoreflect.ValueOfString("1000"))
	require.True(t, proto.Equal(want, got), "inner capacity and unknown fields must remain unchanged")
}

func nestedCapacityFixture(t *testing.T) (*schemapb.FieldSchema, []byte) {
	t.Helper()
	// Raw current-v3 TypeSchema: Array(capacity=1000)<Array(capacity=10)<Int64>>.
	leaf := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), uint64(schemapb.DataType_Int64))
	inner := protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), leaf)
	inner = protowire.AppendBytes(protowire.AppendTag(inner, 3, protowire.BytesType), mustProto(t, capacityParam("10")))
	outer := protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), inner)
	outer = protowire.AppendBytes(protowire.AppendTag(outer, 3, protowire.BytesType), mustProto(t, capacityParam("1000")))
	outer = protowire.AppendVarint(protowire.AppendTag(outer, 70, protowire.VarintType), 99)
	field := &schemapb.FieldSchema{FieldID: 102, Name: "matrix", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array, TypeParams: []*commonpb.KeyValuePair{capacityParam("1000")}}
	field.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 18, protowire.BytesType), outer))
	return field, outer
}

func TestSchemaCapacityRepresentations(t *testing.T) {
	for _, mode := range []string{"missing", "wrong wire type", "duplicate", "bad payload", "not nested", "missing capacity", "bad mirror", "different mirror", "wrong element type"} {
		t.Run(mode, func(t *testing.T) {
			field, _ := nestedCapacityFixture(t)
			switch mode {
			case "missing":
				field.ProtoReflect().SetUnknown(nil)
			case "wrong wire type":
				field.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 18, protowire.VarintType), 1))
			case "duplicate":
				raw := field.ProtoReflect().GetUnknown()
				field.ProtoReflect().SetUnknown(append(append([]byte(nil), raw...), raw...))
			case "bad payload":
				field.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 18, protowire.BytesType), []byte{255}))
			case "not nested":
				field.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 18, protowire.BytesType), nil))
			case "missing capacity":
				root := protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), nil))
				field.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 18, protowire.BytesType), root))
			case "bad mirror":
				field.TypeParams[0].Value = "bad"
			case "different mirror":
				field.TypeParams[0].Value = "2000"
			case "wrong element type":
				field.ElementType = schemapb.DataType_Int64
			}
			_, err := patchArrayCapacity(field, 4096)
			require.Error(t, err)
		})
	}
	t.Run("nested no-op and extra unknown fields", func(t *testing.T) {
		field, _ := nestedCapacityFixture(t)
		raw := protowire.AppendVarint(protowire.AppendTag(nil, 123, protowire.VarintType), 8)
		field.ProtoReflect().SetUnknown(append(raw, field.ProtoReflect().GetUnknown()...))
		before := proto.Clone(field)
		_, err := patchArrayCapacity(field, 1000)
		require.NoError(t, err)
		require.True(t, proto.Equal(before, field))
	})
	t.Run("index params capacity", func(t *testing.T) {
		field := lawyerSchema().Fields[1]
		field.IndexParams = []*commonpb.KeyValuePair{capacityParam("1000")}
		_, err := patchArrayCapacity(field, 4096)
		require.ErrorContains(t, err, "index_params")
	})
	t.Run("invalid member type", func(t *testing.T) {
		schema := lawyerSchema()
		schema.StructArrayFields[0].Fields[0].DataType = schemapb.DataType_VarChar
		_, err := patchSchemaCapacity(schema, "lawyer", 4096)
		require.ErrorContains(t, err, "not an Array")
	})
	t.Run("empty struct", func(t *testing.T) {
		schema := lawyerSchema()
		schema.StructArrayFields[0].Fields = nil
		_, err := patchSchemaCapacity(schema, "lawyer", 4096)
		require.ErrorContains(t, err, "no members")
	})
	t.Run("vector member and parent mirror", func(t *testing.T) {
		schema := lawyerSchema()
		parent := schema.StructArrayFields[0]
		parent.TypeParams = []*commonpb.KeyValuePair{capacityParam("1000")}
		parent.Fields[0].DataType = schemapb.DataType_ArrayOfVector
		parent.Fields[0].ElementType = schemapb.DataType_FloatVector
		parent.Fields[0].TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}, capacityParam("1000")}
		_, err := patchSchemaCapacity(schema, "lawyer", 4096)
		require.NoError(t, err)
		require.Equal(t, "128", parent.Fields[0].TypeParams[0].Value)
		require.Equal(t, "4096", parent.Fields[0].TypeParams[1].Value)
		require.Equal(t, "4096", parent.TypeParams[0].Value)
	})
}

func TestSchemaArgumentsAndBackup(t *testing.T) {
	c := NewComponent(nil, nil, "unused")
	require.ErrorContains(t, c.SchemaCommand(context.Background(), &SchemaParam{}), "provide --collectionID")
	require.ErrorContains(t, c.SchemaCommand(context.Background(), &SchemaParam{CollectionID: 123, Field: "lawyer", MaxCapacity: 4096}), "etcd backend")
	unsupported := kv.NewFileAuditKV(nil, nil)
	_, _, err := unsupported.LoadPrefixAtRevision(context.Background(), "unused", 0)
	require.Error(t, err)
	require.Error(t, unsupported.CommitChanges(context.Background(), nil, nil, nil))
	t.Chdir(t.TempDir())
	name, err := saveCapacityBackup("", &capacityPlan{CollectionID: 123})
	require.NoError(t, err)
	data, err := os.ReadFile(name)
	require.NoError(t, err)
	var plan capacityPlan
	require.NoError(t, json.Unmarshal(data, &plan))
	require.EqualValues(t, 123, plan.CollectionID)
	stat, err := os.Stat(name)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), stat.Mode().Perm())
}

func mustProto(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(message)
	require.NoError(t, err)
	return data
}

type capacityFixture struct {
	cli        *clientv3.Client
	store      kv.MetaKV
	base       string
	collection string
	structKey  string
	latest     []string
	history    []string
	ctx        context.Context
}

// These tests use an explicitly selected local etcd, with a unique prefix and
// prefix-only cleanup. No Milvus process or external instance is required.
func newCapacityFixture(t *testing.T, snapshots bool) *capacityFixture {
	t.Helper()
	endpoint := os.Getenv("BIRDWATCHER_SCHEMA_TEST_ETCD")
	if endpoint == "" {
		t.Skip("set BIRDWATCHER_SCHEMA_TEST_ETCD to a local test etcd endpoint")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	f := &capacityFixture{cli: cli, store: kv.NewEtcdKV(cli), base: "birdwatcher-schema-test/" + uuid.NewString() + "/meta", ctx: ctx}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_, err := cli.Delete(cleanup, strings.TrimSuffix(f.base, "/meta")+"/", clientv3.WithPrefix())
		require.NoError(t, err)
		cancel()
		require.NoError(t, cli.Close())
	})
	schema := lawyerSchema()
	rc := proto.Clone(schema).(*schemapb.CollectionSchema)
	rc.Fields, rc.StructArrayFields = nil, nil
	coll := &etcdpb.CollectionInfo{ID: 123, Schema: rc, VirtualChannelNames: []string{"p1_123v0", "p2_123v1"}, PhysicalChannelNames: []string{"p1", "p2"}}
	f.collection = path.Join(f.base, "root-coord/database/collection-info/1/123")
	f.put(t, f.collection, coll)
	f.structKey = path.Join(f.base, "root-coord/struct-array-fields/123/131")
	f.put(t, f.structKey, schema.StructArrayFields[0])
	for _, field := range schema.Fields {
		f.put(t, path.Join(f.base, "root-coord/fields/123", strconv.FormatInt(field.FieldID, 10)), field)
	}
	if snapshots {
		for _, ts := range []string{"9", "100"} {
			key := path.Join(f.base, "snapshots/root-coord/struct-array-fields/123/131_ts"+ts)
			f.put(t, key, schema.StructArrayFields[0])
			if ts == "9" {
				f.history = append(f.history, key)
			}
		}
	}
	for i, vchannel := range coll.VirtualChannelNames {
		key := path.Join(f.base, "streamingnode-meta/wal", coll.PhysicalChannelNames[i], "vchannel", vchannel)
		f.put(t, key, &streamingpb.VChannelMeta{Vchannel: vchannel, State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CheckpointTimeTick: 200, CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 123}})
		for _, ts := range []uint64{9, 100} {
			version := proto.Clone(schema).(*schemapb.CollectionSchema)
			if ts == 9 {
				version.Version = 6
				version.StructArrayFields[0].Fields[0].TypeParams[1].Value = "50"
			}
			schemaKey := path.Join(key, "schema", strconv.FormatUint(ts, 10))
			f.put(t, schemaKey, &streamingpb.CollectionSchemaOfVChannel{Schema: version, CheckpointTimeTick: ts, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL})
			if ts == 9 {
				f.history = append(f.history, schemaKey)
			} else {
				f.latest = append(f.latest, schemaKey)
			}
		}
	}
	return f
}

func (f *capacityFixture) put(t *testing.T, key string, message proto.Message) {
	t.Helper()
	_, err := f.cli.Put(f.ctx, key, string(mustProto(t, message)))
	require.NoError(t, err)
}

func (f *capacityFixture) all(t *testing.T) map[string]string {
	t.Helper()
	resp, err := f.cli.Get(f.ctx, f.base+"/", clientv3.WithPrefix())
	require.NoError(t, err)
	result := make(map[string]string)
	for _, value := range resp.Kvs {
		result[string(value.Key)] = string(value.Value)
	}
	return result
}

func TestSchemaCommandLocalEtcd(t *testing.T) {
	for _, snapshots := range []bool{false, true} {
		t.Run(strconv.FormatBool(snapshots), func(t *testing.T) {
			f := newCapacityFixture(t, snapshots)
			before := f.all(t)
			c := NewComponent(f.store, &configs.Config{}, f.base)
			backup := filepath.Join(t.TempDir(), "repair.json")
			p := &SchemaParam{CollectionID: 123, Field: "lawyer", MaxCapacity: 4096, Backup: backup}
			require.NoError(t, c.SchemaCommand(f.ctx, p))
			require.Equal(t, before, f.all(t))
			_, err := os.Stat(backup)
			require.True(t, os.IsNotExist(err))
			// Execute via Birdwatcher's real reflected command registration.
			host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
			root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
			host.MergeFunctionCommandsFrom(root, host, c)
			root.SetArgs([]string{"set", "schema", "--collectionID", "123", "--field", "lawyer", "--max-capacity", "4096", "--backup", backup, "--run"})
			require.NoError(t, root.Execute())
			after := f.all(t)
			rcStruct := &schemapb.StructArrayFieldSchema{}
			require.NoError(t, proto.Unmarshal([]byte(after[f.structKey]), rcStruct))
			for _, member := range rcStruct.Fields {
				require.Equal(t, "4096", member.TypeParams[1].Value)
				member.TypeParams[1].Value = "1000"
			}
			require.True(t, proto.Equal(lawyerSchema().StructArrayFields[0], rcStruct))
			data, err := os.ReadFile(backup)
			require.NoError(t, err)
			var plan capacityPlan
			require.NoError(t, json.Unmarshal(data, &plan))
			wantCount := 3
			if snapshots {
				wantCount++
			}
			require.Len(t, plan.Changes, wantCount)
			for _, change := range plan.Changes {
				require.Equal(t, before[change.Before.Key], string(change.Before.Value))
				require.Equal(t, after[change.Before.Key], string(change.After))
				delete(before, change.Before.Key)
				delete(after, change.Before.Key)
			}
			require.Equal(t, before, after, "all non-target metadata and historical schemas must be byte-identical")
			for _, key := range f.latest {
				data, err := f.store.Load(f.ctx, key)
				require.NoError(t, err)
				got := &streamingpb.CollectionSchemaOfVChannel{}
				require.NoError(t, proto.Unmarshal([]byte(data), got))
				require.EqualValues(t, 100, got.CheckpointTimeTick)
				require.EqualValues(t, 7, got.Schema.Version)
				for _, member := range got.Schema.StructArrayFields[0].Fields {
					require.Equal(t, "4096", member.TypeParams[1].Value)
					member.TypeParams[1].Value = "1000"
				}
				require.True(t, proto.Equal(lawyerSchema(), got.Schema))
			}
			before = f.all(t)
			p.Run = true
			require.NoError(t, c.SchemaCommand(f.ctx, p), "no-op must not try to overwrite existing backup")
			require.Equal(t, before, f.all(t))
		})
	}
}

func TestSchemaRejectsIncompleteLocalEtcd(t *testing.T) {
	for _, mode := range []string{"missing shard", "unpublished", "different version", "backup exists"} {
		t.Run(mode, func(t *testing.T) {
			f := newCapacityFixture(t, false)
			p := &SchemaParam{CollectionID: 123, Field: "lawyer", MaxCapacity: 4096, Backup: filepath.Join(t.TempDir(), "backup.json")}
			p.Run = true
			switch mode {
			case "missing shard":
				_, err := f.cli.Delete(f.ctx, path.Dir(path.Dir(f.latest[1])), clientv3.WithPrefix())
				require.NoError(t, err)
			case "unpublished":
				f.put(t, path.Join(path.Dir(f.latest[0]), "201"), &streamingpb.CollectionSchemaOfVChannel{Schema: lawyerSchema(), CheckpointTimeTick: 201, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL})
			case "different version":
				schema := lawyerSchema()
				schema.Version = 8
				f.put(t, f.latest[0], &streamingpb.CollectionSchemaOfVChannel{Schema: schema, CheckpointTimeTick: 100, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL})
			case "backup exists":
				require.NoError(t, os.WriteFile(p.Backup, []byte("keep"), 0o600))
			}
			before := f.all(t)
			require.Error(t, NewComponent(f.store, &configs.Config{}, f.base).SchemaCommand(f.ctx, p))
			require.Equal(t, before, f.all(t))
		})
	}
}

func TestSchemaConditionalTransactionLocalEtcd(t *testing.T) {
	for _, mode := range []string{"target changed", "new schema", "guard deleted"} {
		t.Run(mode, func(t *testing.T) {
			f := newCapacityFixture(t, false)
			cli := f.store.(kv.ConditionalKV)
			plan, err := buildCapacityPlan(f.ctx, cli, f.base, &SchemaParam{CollectionID: 123, Field: "lawyer", MaxCapacity: 4096})
			require.NoError(t, err)
			switch mode {
			case "target changed":
				f.put(t, f.structKey, lawyerSchema().StructArrayFields[0]) // Same bytes, different revision.
			case "new schema":
				f.put(t, path.Join(path.Dir(f.latest[0]), "150"), &streamingpb.CollectionSchemaOfVChannel{Schema: lawyerSchema(), CheckpointTimeTick: 150, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL})
			case "guard deleted":
				_, err := f.cli.Delete(f.ctx, f.collection)
				require.NoError(t, err)
			}
			before := f.all(t)
			require.ErrorContains(t, cli.CommitChanges(f.ctx, plan.Guards, plan.Prefixes, plan.Changes), "metadata changed")
			require.Equal(t, before, f.all(t), "conditional failure must not partially update RC or SN")
		})
	}
}

func TestSchemaTransactionValidationLocalEtcd(t *testing.T) {
	f := newCapacityFixture(t, false)
	cli := f.store.(kv.ConditionalKV)
	p := &SchemaParam{CollectionID: 123, Field: "lawyer", MaxCapacity: 4096}
	plan, err := buildCapacityPlan(f.ctx, cli, f.base, p)
	require.NoError(t, err)
	before := f.all(t)
	require.NoError(t, cli.CommitChanges(f.ctx, nil, nil, nil))
	change := plan.Changes[0]
	require.ErrorContains(t, cli.CommitChanges(f.ctx, []kv.RevisionedValue{{Key: change.Before.Key}}, nil, []kv.ConditionalChange{change}), "missing revision")
	require.ErrorContains(t, cli.CommitChanges(f.ctx, nil, nil, []kv.ConditionalChange{change, change}), "duplicate change")
	guard := change.Before
	guard.ModRevision++
	require.ErrorContains(t, cli.CommitChanges(f.ctx, []kv.RevisionedValue{guard}, nil, []kv.ConditionalChange{change}), "conflicting revisions")
	require.ErrorContains(t, cli.CommitChanges(f.ctx, nil, []kv.PrefixRevision{{Prefix: f.base}}, []kv.ConditionalChange{change}), "missing snapshot revision")
	var large []kv.ConditionalChange
	for i := 0; i < 65; i++ {
		entry := change
		entry.Before.Key = path.Join(f.base, "large", strconv.Itoa(i))
		large = append(large, entry)
	}
	require.ErrorContains(t, cli.CommitChanges(f.ctx, nil, nil, large), "single-transaction limit")
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	require.Error(t, cli.CommitChanges(canceled, plan.Guards, plan.Prefixes, plan.Changes))
	require.Error(t, NewComponent(f.store, &configs.Config{}, f.base).SchemaCommand(canceled, p))
	require.Error(t, verifyCapacityChanges(canceled, cli, plan.Changes))
	require.ErrorContains(t, verifyCapacityChanges(f.ctx, cli, plan.Changes), "unexpected value")
	require.Equal(t, before, f.all(t))
}

func TestSchemaArrayAndEmbeddedLocalEtcd(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		t.Run(strconv.FormatBool(embedded), func(t *testing.T) {
			f := newCapacityFixture(t, false)
			if embedded {
				coll := &etcdpb.CollectionInfo{}
				require.NoError(t, proto.Unmarshal([]byte(f.all(t)[f.collection]), coll))
				coll.Schema = lawyerSchema()
				f.put(t, f.collection, coll)
				for _, prefix := range []string{"root-coord/fields/123/", "root-coord/struct-array-fields/123/"} {
					_, err := f.cli.Delete(f.ctx, path.Join(f.base, prefix)+"/", clientv3.WithPrefix())
					require.NoError(t, err)
				}
			}
			before := f.all(t)
			// Exercise the audit wrapper, not just the bare etcd client.
			file, err := os.CreateTemp(t.TempDir(), "audit-*")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, file.Close()) })
			store := kv.NewFileAuditKV(f.store, file)
			p := &SchemaParam{CollectionID: 123, Field: "tags", MaxCapacity: 4096, Backup: filepath.Join(t.TempDir(), "repair.json")}
			p.Run = true
			require.NoError(t, NewComponent(store, &configs.Config{}, f.base).SchemaCommand(f.ctx, p))
			after := f.all(t)
			var rcField *schemapb.FieldSchema
			if embedded {
				coll := &etcdpb.CollectionInfo{}
				require.NoError(t, proto.Unmarshal([]byte(after[f.collection]), coll))
				rcField = coll.Schema.Fields[1]
			} else {
				rcField = &schemapb.FieldSchema{}
				require.NoError(t, proto.Unmarshal([]byte(after[path.Join(f.base, "root-coord/fields/123/101")]), rcField))
			}
			require.Equal(t, "4096", rcField.TypeParams[0].Value)
			for _, key := range f.history {
				require.Equal(t, before[key], after[key])
			}
			stat, err := file.Stat()
			require.NoError(t, err)
			require.Positive(t, stat.Size())
		})
	}
}

func TestSchemaCorruptMetadataLocalEtcd(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *capacityFixture)
		want   string
	}{
		{"collection missing", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Delete(f.ctx, f.collection)
			require.NoError(t, err)
		}, "not found"},
		{"collection duplicate", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Put(f.ctx, path.Join(f.base, "root-coord/collection/123"), f.all(t)[f.collection])
			require.NoError(t, err)
		}, "multiple RootCoord"},
		{"collection unreadable", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Put(f.ctx, f.collection, "broken")
			require.NoError(t, err)
		}, "decode"},
		{"collection dropping", func(t *testing.T, f *capacityFixture) {
			coll := &etcdpb.CollectionInfo{}
			require.NoError(t, proto.Unmarshal([]byte(f.all(t)[f.collection]), coll))
			coll.State = etcdpb.CollectionState_CollectionDropping
			f.put(t, f.collection, coll)
		}, "in state"},
		{"mixed layout", func(t *testing.T, f *capacityFixture) {
			coll := &etcdpb.CollectionInfo{}
			require.NoError(t, proto.Unmarshal([]byte(f.all(t)[f.collection]), coll))
			coll.Schema.Fields = lawyerSchema().Fields
			f.put(t, f.collection, coll)
		}, "mixed embedded"},
		{"field key unreadable", func(t *testing.T, f *capacityFixture) {
			f.put(t, path.Join(f.base, "root-coord/fields/123/not-a-number"), lawyerSchema().Fields[0])
		}, "invalid field key"},
		{"field unreadable", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Put(f.ctx, path.Join(f.base, "root-coord/fields/123/101"), "broken")
			require.NoError(t, err)
		}, "decode"},
		{"struct unreadable", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Put(f.ctx, f.structKey, "broken")
			require.NoError(t, err)
		}, "decode"},
		{"duplicate ID", func(t *testing.T, f *capacityFixture) {
			f.put(t, path.Join(f.base, "root-coord/fields/123/131"), &schemapb.FieldSchema{FieldID: 131, Name: "bad"})
		}, "duplicate RootCoord"},
		{"field ID mismatch", func(t *testing.T, f *capacityFixture) {
			f.put(t, path.Join(f.base, "root-coord/fields/123/101"), &schemapb.FieldSchema{FieldID: 999, Name: "bad"})
		}, "field ID mismatch"},
		{"struct ID mismatch", func(t *testing.T, f *capacityFixture) {
			f.put(t, f.structKey, &schemapb.StructArrayFieldSchema{FieldID: 999, Name: "lawyer"})
		}, "StructArray ID mismatch"},
		{"snapshot unreadable", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Put(f.ctx, path.Join(f.base, "snapshots/root-coord/struct-array-fields/123/131_ts100"), "broken")
			require.NoError(t, err)
		}, "decode"},
		{"snapshot mismatch", func(t *testing.T, f *capacityFixture) {
			value := lawyerSchema().StructArrayFields[0]
			value.Description = "different"
			f.put(t, path.Join(f.base, "snapshots/root-coord/struct-array-fields/123/131_ts100"), value)
		}, "snapshot differs"},
		{"snapshot key unreadable", func(t *testing.T, f *capacityFixture) {
			f.put(t, path.Join(f.base, "snapshots/root-coord/struct-array-fields/123/131_tsbad"), lawyerSchema().StructArrayFields[0])
		}, "invalid snapshot key"},
		{"SN schema missing", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Delete(f.ctx, path.Dir(f.latest[0])+"/", clientv3.WithPrefix())
			require.NoError(t, err)
		}, "no StreamingNode schema"},
		{"SN schema unreadable", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Put(f.ctx, f.latest[0], "broken")
			require.NoError(t, err)
		}, "decode"},
		{"SN schema key unreadable", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Put(f.ctx, path.Join(path.Dir(f.latest[0]), "bad"), "broken")
			require.NoError(t, err)
		}, "invalid schema key"},
		{"SN timestamp mismatch", func(t *testing.T, f *capacityFixture) {
			f.put(t, f.latest[0], &streamingpb.CollectionSchemaOfVChannel{Schema: lawyerSchema(), CheckpointTimeTick: 99, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL})
		}, "invalid latest schema"},
		{"SN target mismatch", func(t *testing.T, f *capacityFixture) {
			schema := lawyerSchema()
			schema.StructArrayFields[0].Description = "different"
			f.put(t, f.latest[0], &streamingpb.CollectionSchemaOfVChannel{Schema: schema, CheckpointTimeTick: 100, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL})
		}, "target fields differ"},
		{"SN member missing", func(t *testing.T, f *capacityFixture) {
			schema := lawyerSchema()
			schema.StructArrayFields = nil
			f.put(t, f.latest[0], &streamingpb.CollectionSchemaOfVChannel{Schema: schema, CheckpointTimeTick: 100, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL})
		}, "not found"},
		{"vchannel unreadable", func(t *testing.T, f *capacityFixture) {
			_, err := f.cli.Put(f.ctx, path.Dir(path.Dir(f.latest[0])), "broken")
			require.NoError(t, err)
		}, "decode"},
		{"vchannel embedded schemas", func(t *testing.T, f *capacityFixture) {
			key := path.Dir(path.Dir(f.latest[0]))
			channel := &streamingpb.VChannelMeta{}
			require.NoError(t, proto.Unmarshal([]byte(f.all(t)[key]), channel))
			channel.CollectionInfo.Schemas = []*streamingpb.CollectionSchemaOfVChannel{{Schema: lawyerSchema()}}
			f.put(t, key, channel)
		}, "embedded SN schema"},
		{"channel mapping missing", func(t *testing.T, f *capacityFixture) {
			coll := &etcdpb.CollectionInfo{}
			require.NoError(t, proto.Unmarshal([]byte(f.all(t)[f.collection]), coll))
			coll.PhysicalChannelNames = nil
			f.put(t, f.collection, coll)
		}, "channel mappings"},
		{"channel mapping duplicate", func(t *testing.T, f *capacityFixture) {
			coll := &etcdpb.CollectionInfo{}
			require.NoError(t, proto.Unmarshal([]byte(f.all(t)[f.collection]), coll))
			coll.VirtualChannelNames[1] = coll.VirtualChannelNames[0]
			f.put(t, f.collection, coll)
		}, "duplicate vchannel"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			f := newCapacityFixture(t, false)
			tt.mutate(t, f)
			before := f.all(t)
			p := &SchemaParam{CollectionID: 123, Field: "lawyer", MaxCapacity: 4096, Backup: filepath.Join(t.TempDir(), "backup.json")}
			p.Run = true
			require.ErrorContains(t, NewComponent(f.store, &configs.Config{}, f.base).SchemaCommand(f.ctx, p), tt.want)
			require.Equal(t, before, f.all(t))
			_, err := os.Stat(p.Backup)
			require.True(t, os.IsNotExist(err))
		})
	}
}
