package repair

import (
	"context"
	"fmt"
	"path"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

type RepairCollectionMetaFieldParam struct {
	framework.ExecutionParam `use:"repair collection-meta-field" desc:"scan and fix $meta field with missing Nullable=true and DefaultValue settings (2.5 -> 2.6 upgrade compatibility)"`
}

func (c *ComponentRepair) RepairCollectionMetaFieldCommand(ctx context.Context, p *RepairCollectionMetaFieldParam) error {
	mismatchCount := 0
	fixedCount := 0

	// Phase 1: Fix rootcoord field schemas (separated storage)
	rc, fc, err := c.repairRootCoordMetaFields(ctx, p)
	if err != nil {
		return err
	}
	mismatchCount += rc
	fixedCount += fc

	// Phase 2: Fix streaming node catalog schemas (inline storage)
	rc, fc, err = c.repairStreamingSchemas(ctx, p)
	if err != nil {
		return err
	}
	mismatchCount += rc
	fixedCount += fc

	// Summary
	fmt.Println()
	if mismatchCount == 0 {
		fmt.Println("All $meta fields are correct. No fix needed.")
	} else if p.Run {
		fmt.Printf("Total: %d issue(s) found, %d fixed successfully.\n", mismatchCount, fixedCount)
	} else {
		fmt.Printf("Total: %d issue(s) found. Re-run with --run to apply changes.\n", mismatchCount)
	}

	return nil
}

// metaFieldNeedsFix checks if a $meta FieldSchema needs Nullable/DefaultValue fix.
// Returns (needsFix, reasons).
func metaFieldNeedsFix(field *schemapb.FieldSchema) (bool, []string) {
	needsFix := false
	var reasons []string

	if !field.GetNullable() {
		needsFix = true
		reasons = append(reasons, "Nullable=false (expected true)")
	}

	dv := field.GetDefaultValue()
	if dv == nil {
		needsFix = true
		reasons = append(reasons, "DefaultValue=nil (expected \"{}\")")
	} else if bytesVal, ok := dv.GetData().(*schemapb.ValueField_BytesData); !ok || string(bytesVal.BytesData) != "{}" {
		needsFix = true
		reasons = append(reasons, fmt.Sprintf("DefaultValue=%v (expected \"{}\")", dv))
	}

	return needsFix, reasons
}

// applyMetaFieldFix sets Nullable=true and DefaultValue="{}" on the field.
func applyMetaFieldFix(field *schemapb.FieldSchema) {
	field.Nullable = true
	field.DefaultValue = &schemapb.ValueField{
		Data: &schemapb.ValueField_BytesData{
			BytesData: []byte("{}"),
		},
	}
}

// repairRootCoordMetaFields scans and fixes $meta fields in rootcoord's separated field storage.
func (c *ComponentRepair) repairRootCoordMetaFields(ctx context.Context, p *RepairCollectionMetaFieldParam) (int, int, error) {
	collections, err := common.ListCollectionWithoutFields(ctx, c.client, c.basePath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list collections: %w", err)
	}

	if len(collections) == 0 {
		fmt.Println("[rootcoord] No collections found.")
		return 0, 0, nil
	}

	fmt.Printf("[rootcoord] Found %d collection(s), scanning for $meta field issues...\n", len(collections))

	mismatchCount := 0
	fixedCount := 0

	for _, coll := range collections {
		collProto := coll.GetProto()
		collName := collProto.GetSchema().GetName()
		collID := collProto.GetID()

		if !collProto.GetSchema().GetEnableDynamicField() {
			continue
		}

		// Load fields with their etcd keys
		prefix := path.Join(c.basePath, fmt.Sprintf("root-coord/fields/%d", collID))
		fields, keys, err := common.ListProtoObjects[schemapb.FieldSchema](ctx, c.client, prefix)
		if err != nil {
			fmt.Printf("  [error] failed to list fields for collection %d (%s): %s\n", collID, collName, err.Error())
			continue
		}

		// Find the $meta dynamic field and its key
		var metaField *schemapb.FieldSchema
		var metaFieldKey string
		for i, field := range fields {
			if field.GetIsDynamic() {
				metaField = field
				metaFieldKey = keys[i]
				break
			}
		}

		if metaField == nil {
			continue
		}

		needsFix, reasons := metaFieldNeedsFix(metaField)
		if !needsFix {
			continue
		}

		mismatchCount++
		fmt.Printf("\n[rootcoord] Collection %d (%s): $meta field (fieldID=%d) needs fix:\n", collID, collName, metaField.GetFieldID())
		for _, reason := range reasons {
			fmt.Printf("  - %s\n", reason)
		}

		if !p.Run {
			fmt.Printf("  [dry-run] would set Nullable=true, DefaultValue=\"{}\"\n")
			continue
		}

		applyMetaFieldFix(metaField)

		bs, err := proto.Marshal(metaField)
		if err != nil {
			fmt.Printf("  [error] failed to marshal field: %s\n", err.Error())
			continue
		}
		if err := c.client.Save(ctx, metaFieldKey, string(bs)); err != nil {
			fmt.Printf("  [error] failed to save field to etcd: %s\n", err.Error())
			continue
		}
		fmt.Printf("  [fixed] $meta field updated: Nullable=true, DefaultValue=\"{}\"\n")
		fixedCount++
	}

	return mismatchCount, fixedCount, nil
}

const (
	streamingWALPrefix        = "streamingnode-meta/wal/"
	streamingVChannelDir      = "vchannel"
	streamingSchemaDir        = "schema"
	streamingCoordPChannelDir = "streamingcoord-meta/pchannel/"
)

// repairStreamingSchemas scans and fixes $meta fields in streaming node catalog schemas.
func (c *ComponentRepair) repairStreamingSchemas(ctx context.Context, p *RepairCollectionMetaFieldParam) (int, int, error) {
	// List all pchannels from streaming coord
	pchannels, _, err := common.ListProtoObjects[streamingpb.PChannelMeta](ctx, c.client, path.Join(c.basePath, streamingCoordPChannelDir))
	if err != nil {
		// Streaming may not be enabled, skip silently
		fmt.Println("[streaming] No streaming metadata found, skipping.")
		return 0, 0, nil
	}

	if len(pchannels) == 0 {
		fmt.Println("[streaming] No pchannels found, skipping.")
		return 0, 0, nil
	}

	fmt.Printf("[streaming] Found %d pchannel(s), scanning for $meta field issues in schemas...\n", len(pchannels))

	mismatchCount := 0
	fixedCount := 0

	for _, pchannel := range pchannels {
		pchannelName := pchannel.GetChannel().GetName()
		prefix := path.Join(c.basePath, streamingWALPrefix, pchannelName) + "/"

		keys, vals, err := c.client.LoadWithPrefix(ctx, prefix)
		if err != nil {
			fmt.Printf("  [error] failed to load streaming meta for pchannel %s: %s\n", pchannelName, err.Error())
			continue
		}

		for idx, key := range keys {
			// Match schema keys: {prefix}vchannel/{vchannelName}/schema/{timeTick}
			relKey := strings.TrimPrefix(key, prefix)
			parts := strings.Split(relKey, "/")
			if len(parts) != 4 || parts[0] != streamingVChannelDir || parts[2] != streamingSchemaDir {
				continue
			}

			vchannelName := parts[1]

			schema := &streamingpb.CollectionSchemaOfVChannel{}
			if err := proto.Unmarshal([]byte(vals[idx]), schema); err != nil {
				fmt.Printf("  [error] failed to unmarshal schema at %s: %s\n", key, err.Error())
				continue
			}

			collSchema := schema.GetSchema()
			if collSchema == nil || !collSchema.GetEnableDynamicField() {
				continue
			}

			// Find $meta field in the inline schema
			var metaField *schemapb.FieldSchema
			for _, field := range collSchema.GetFields() {
				if field.GetIsDynamic() {
					metaField = field
					break
				}
			}

			if metaField == nil {
				continue
			}

			needsFix, reasons := metaFieldNeedsFix(metaField)
			if !needsFix {
				continue
			}

			mismatchCount++
			fmt.Printf("\n[streaming] pchannel=%s vchannel=%s schema@%d: $meta field needs fix:\n",
				pchannelName, vchannelName, schema.GetCheckpointTimeTick())
			for _, reason := range reasons {
				fmt.Printf("  - %s\n", reason)
			}

			if !p.Run {
				fmt.Printf("  [dry-run] would set Nullable=true, DefaultValue=\"{}\"\n")
				continue
			}

			applyMetaFieldFix(metaField)

			bs, err := proto.Marshal(schema)
			if err != nil {
				fmt.Printf("  [error] failed to marshal schema: %s\n", err.Error())
				continue
			}
			if err := c.client.Save(ctx, key, string(bs)); err != nil {
				fmt.Printf("  [error] failed to save schema to etcd: %s\n", err.Error())
				continue
			}
			fmt.Printf("  [fixed] schema updated: Nullable=true, DefaultValue=\"{}\"\n")
			fixedCount++
		}
	}

	return mismatchCount, fixedCount, nil
}
