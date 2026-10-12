package set

import (
	"fmt"
	"strconv"
	"sync"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// FieldSchema.type_schema (18) is unknown to Birdwatcher's v2 bindings. Decode
// just this message with a private descriptor; do not import conflicting v3
// generated registrations. Wire numbers match Milvus schema.proto ae7fea6ab2f4.
var capacityTypeDescriptor = sync.OnceValue(func() protoreflect.MessageDescriptor {
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	message := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	registry := new(protoregistry.Files)
	if err := registry.RegisterFile(commonpb.File_common_proto); err != nil {
		panic(err)
	}
	// A private package avoids even local name collisions with generated types.
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("birdwatcher_capacity_type.proto"), Package: proto.String("birdwatcher.capacity"), Syntax: proto.String("proto3"),
		Dependency: []string{commonpb.File_common_proto.Path()},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:      proto.String("TypeSchema"),
			OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("kind")}},
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("leaf_type"), Number: proto.Int32(1), Label: &optional, Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), OneofIndex: proto.Int32(0)},
				{Name: proto.String("array_element"), Number: proto.Int32(2), Label: &optional, Type: &message, TypeName: proto.String(".birdwatcher.capacity.TypeSchema"), OneofIndex: proto.Int32(0)},
				{Name: proto.String("type_params"), Number: proto.Int32(3), Label: &repeated, Type: &message, TypeName: proto.String(".milvus.proto.common.KeyValuePair")},
				{Name: proto.String("nullable"), Number: proto.Int32(4), Label: &optional, Type: descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()},
			},
		}},
	}, registry)
	if err != nil {
		panic(err) // Static descriptor definition, independent of user metadata.
	}
	return file.Messages().ByName("TypeSchema")
})

func hasTypeSchema(field *schemapb.FieldSchema) bool {
	for raw := field.ProtoReflect().GetUnknown(); len(raw) > 0; {
		num, _, size := protowire.ConsumeField(raw)
		if size < 0 || num == 18 {
			return true
		}
		raw = raw[size:]
	}
	return false
}

func patchNestedCapacity(field *schemapb.FieldSchema, capacity int64) (int64, error) {
	raw := field.ProtoReflect().GetUnknown()
	var rewritten []byte
	var old int64
	for len(raw) > 0 {
		num, typ, size := protowire.ConsumeField(raw)
		if size < 0 {
			return 0, protowire.ParseError(size)
		}
		if num != 18 {
			rewritten = append(rewritten, raw[:size]...)
		} else {
			if old != 0 || typ != protowire.BytesType {
				return 0, fmt.Errorf("invalid or duplicate type_schema")
			}
			_, _, tagLen := protowire.ConsumeTag(raw)
			payload, n := protowire.ConsumeBytes(raw[tagLen:size])
			if n < 0 {
				return 0, protowire.ParseError(n)
			}
			root := dynamicpb.NewMessage(capacityTypeDescriptor())
			if err := proto.Unmarshal(payload, root); err != nil {
				return 0, err
			}
			element := root.Descriptor().Fields().ByNumber(2)
			if !root.Has(element) || !root.Get(element).Message().Has(element) {
				return 0, fmt.Errorf("type_schema does not describe a nested Array")
			}
			params := root.Get(root.Descriptor().Fields().ByNumber(3)).List()
			var pairs []*commonpb.KeyValuePair
			for i := 0; i < params.Len(); i++ {
				entry := params.Get(i).Message()
				pairs = append(pairs, &commonpb.KeyValuePair{Key: entry.Get(entry.Descriptor().Fields().ByNumber(1)).String(), Value: entry.Get(entry.Descriptor().Fields().ByNumber(2)).String()})
			}
			var err error
			old, err = patchCapacity(pairs, capacity, true)
			if err != nil {
				return 0, err
			}
			for i, pair := range pairs {
				if pair.Key == "max_capacity" {
					entry := params.Get(i).Message()
					entry.Set(entry.Descriptor().Fields().ByNumber(2), protoreflect.ValueOfString(strconv.FormatInt(capacity, 10)))
				}
			}
			payload, err = proto.Marshal(root)
			if err != nil {
				return 0, err
			}
			rewritten = protowire.AppendTag(rewritten, 18, protowire.BytesType)
			rewritten = protowire.AppendBytes(rewritten, payload)
		}
		raw = raw[size:]
	}
	if old == 0 {
		return 0, fmt.Errorf("nested Array is missing type_schema")
	}
	mirror, err := patchCapacity(field.TypeParams, capacity, false)
	if err != nil {
		return 0, err
	}
	if mirror != 0 && mirror != old {
		return 0, fmt.Errorf("nested Array capacity mirror %d differs from root capacity %d", mirror, old)
	}
	if old != capacity {
		field.ProtoReflect().SetUnknown(rewritten)
	}
	return old, nil
}
