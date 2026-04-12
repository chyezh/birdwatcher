package repair

import (
	"context"
	"fmt"
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type RepairCollectionMetaFieldParam struct {
	framework.ExecutionParam `use:"repair collection-meta-field" desc:"scan and fix $meta field with missing Nullable=true and DefaultValue settings (2.5 -> 2.6 upgrade compatibility)"`
}

func (c *ComponentRepair) RepairCollectionMetaFieldCommand(ctx context.Context, p *RepairCollectionMetaFieldParam) error {
	collections, err := common.ListCollectionWithoutFields(ctx, c.client, c.basePath)
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	if len(collections) == 0 {
		fmt.Println("No collections found.")
		return nil
	}

	fmt.Printf("Found %d collection(s), scanning for $meta field issues...\n", len(collections))

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
			fmt.Printf("[error] failed to list fields for collection %d (%s): %s\n", collID, collName, err.Error())
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
			fmt.Printf("[warn] collection %d (%s) has EnableDynamicField=true but no dynamic field found, skipping\n", collID, collName)
			continue
		}

		needsFix := false
		var reasons []string

		// Check Nullable
		if !metaField.GetNullable() {
			needsFix = true
			reasons = append(reasons, "Nullable=false (expected true)")
		}

		// Check DefaultValue - should be BytesData with "{}"
		dv := metaField.GetDefaultValue()
		if dv == nil {
			needsFix = true
			reasons = append(reasons, "DefaultValue=nil (expected \"{}\")")
		} else if bytesVal, ok := dv.GetData().(*schemapb.ValueField_BytesData); !ok || string(bytesVal.BytesData) != "{}" {
			needsFix = true
			reasons = append(reasons, fmt.Sprintf("DefaultValue=%v (expected \"{}\")", dv))
		}

		if !needsFix {
			continue
		}

		mismatchCount++
		fmt.Printf("\nCollection %d (%s): $meta field (fieldID=%d) needs fix:\n", collID, collName, metaField.GetFieldID())
		for _, reason := range reasons {
			fmt.Printf("  - %s\n", reason)
		}

		if !p.Run {
			fmt.Printf("  [dry-run] would set Nullable=true, DefaultValue=\"{}\"\n")
			continue
		}

		// Apply fix
		metaField.Nullable = true
		metaField.DefaultValue = &schemapb.ValueField{
			Data: &schemapb.ValueField_BytesData{
				BytesData: []byte("{}"),
			},
		}

		// Save the field back to etcd using the original key
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

	fmt.Println()
	if mismatchCount == 0 {
		fmt.Println("All collections have correct $meta field configuration. No fix needed.")
	} else if p.Run {
		fmt.Printf("Total: %d collection(s) needed fix, %d fixed successfully.\n", mismatchCount, fixedCount)
	} else {
		fmt.Printf("Total: %d collection(s) need fix. Re-run with --run to apply changes.\n", mismatchCount)
	}

	return nil
}
