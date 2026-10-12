package set

import (
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

const maxRepairCapacity int64 = 4096

type capacityMember struct {
	ID   int64
	Name string
	Old  int64
}

type capacityTarget struct {
	ID      int64
	Name    string
	Message proto.Message
	Members []capacityMember
}

// patchCapacity changes only an existing capacity parameter. Unknown fields,
// parameter order, nullable, dimensions and max_length are preserved.
func patchCapacity(params []*commonpb.KeyValuePair, capacity int64, required bool) (int64, error) {
	var target *commonpb.KeyValuePair
	for _, param := range params {
		if param.GetKey() == "max_capacity" {
			if target != nil {
				return 0, fmt.Errorf("duplicate max_capacity parameter")
			}
			target = param
		}
	}
	if target == nil {
		if required {
			return 0, fmt.Errorf("missing max_capacity parameter")
		}
		return 0, nil
	}
	old, err := strconv.ParseInt(target.Value, 10, 64)
	if err != nil || old <= 0 {
		return 0, fmt.Errorf("invalid current max_capacity %q", target.Value)
	}
	if capacity < old {
		return 0, fmt.Errorf("cannot reduce max_capacity from %d to %d without validating stored data", old, capacity)
	}
	if capacity != old {
		target.Value = strconv.FormatInt(capacity, 10)
	}
	return old, nil
}

func patchArrayCapacity(field *schemapb.FieldSchema, capacity int64) (int64, error) {
	if field.GetDataType() != schemapb.DataType_Array && field.GetDataType() != schemapb.DataType_ArrayOfVector {
		return 0, fmt.Errorf("field %s (%d) is not an Array or ArrayOfVector", field.GetName(), field.GetFieldID())
	}
	// An index parameter with the same key takes precedence in some readers.
	// Do not silently leave a second, effective capacity behind.
	for _, param := range field.GetIndexParams() {
		if param.GetKey() == "max_capacity" {
			return 0, fmt.Errorf("field %s has an unsupported max_capacity in index_params", field.GetName())
		}
	}
	if field.GetElementType() == schemapb.DataType_Array {
		return patchNestedCapacity(field, capacity)
	}
	if hasTypeSchema(field) {
		return 0, fmt.Errorf("field %s has type_schema without nested Array element type", field.GetName())
	}
	return patchCapacity(field.TypeParams, capacity, true)
}

// patchSchemaCapacity receives a private clone and binds the name to a stable
// top-level field ID. A StructArray is one logical capacity change.
func patchSchemaCapacity(schema *schemapb.CollectionSchema, name string, capacity int64) (*capacityTarget, error) {
	if capacity <= 0 || capacity > maxRepairCapacity {
		return nil, fmt.Errorf("max-capacity must be in [1, %d]", maxRepairCapacity)
	}
	var target *capacityTarget
	for _, field := range schema.GetFields() {
		if field.GetName() == name {
			if target != nil {
				return nil, fmt.Errorf("ambiguous field name %q", name)
			}
			target = &capacityTarget{ID: field.FieldID, Name: name, Message: field}
		}
	}
	for _, field := range schema.GetStructArrayFields() {
		if field.GetName() == name {
			if target != nil {
				return nil, fmt.Errorf("ambiguous field name %q", name)
			}
			target = &capacityTarget{ID: field.FieldID, Name: name, Message: field}
		}
	}
	if target == nil {
		return nil, fmt.Errorf("top-level field %q not found; specify the parent StructArray to change its capacity", name)
	}
	var fields []*schemapb.FieldSchema
	switch field := target.Message.(type) {
	case *schemapb.FieldSchema:
		if field.DataType != schemapb.DataType_Array {
			return nil, fmt.Errorf("top-level field %s is not an Array", name)
		}
		fields = []*schemapb.FieldSchema{field}
	case *schemapb.StructArrayFieldSchema:
		fields = field.Fields
		if len(fields) == 0 {
			return nil, fmt.Errorf("StructArray %s has no members", name)
		}
	}
	ids := make(map[int64]bool, len(fields))
	var expected int64
	for _, field := range fields {
		if field.GetFieldID() <= 0 || ids[field.GetFieldID()] {
			return nil, fmt.Errorf("invalid or duplicate member field ID %d", field.GetFieldID())
		}
		ids[field.FieldID] = true
		old, err := patchArrayCapacity(field, capacity)
		if err != nil {
			return nil, fmt.Errorf("field %s (%d): %w", field.Name, field.FieldID, err)
		}
		if expected != 0 && old != expected {
			return nil, fmt.Errorf("StructArray %s has inconsistent member capacities: %d and %d", name, expected, old)
		}
		expected = old
		target.Members = append(target.Members, capacityMember{ID: field.FieldID, Name: field.Name, Old: old})
	}
	if field, ok := target.Message.(*schemapb.StructArrayFieldSchema); ok {
		old, err := patchCapacity(field.TypeParams, capacity, false)
		if err != nil {
			return nil, err
		}
		if old != 0 && old != expected {
			return nil, fmt.Errorf("StructArray %s capacity differs from its members", name)
		}
	}
	return target, nil
}
