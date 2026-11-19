// Package export converts column-based WAL inserts to Milvus bulk-import rows.
package export

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// WriteInsert writes one JSON object per row, using the collection's import field names.
func WriteInsert(w io.Writer, schema *schemapb.CollectionSchema, body *msgpb.InsertRequest) error {
	if len(body.GetFieldsData()) == 0 || len(body.GetRowData()) != 0 || body.GetNumRows() > math.MaxInt {
		return errors.New("expected a column-based insert")
	}
	if schema == nil {
		return errors.New("collection schema is missing")
	}
	fields := make(map[int64]*schemapb.FieldSchema)
	for _, f := range typeutil.GetAllFieldSchemas(schema) {
		fields[f.GetFieldID()] = f
	}
	columns := make(map[string]readValue)
	for _, fd := range body.GetFieldsData() {
		id := fd.GetFieldId()
		if common.IsSystemField(id) {
			continue
		}
		f := fields[id]
		if f == nil || f.GetDataType() != fd.GetType() {
			return errors.Newf("WAL field %d (%s) does not match collection schema", id, fd.GetType())
		}
		if f.GetIsFunctionOutput() {
			continue
		}
		if _, exists := columns[f.GetName()]; exists {
			return errors.Newf("duplicate field %q", f.GetName())
		}
		read, err := columnReader(fd, int(body.GetNumRows()))
		if err != nil {
			return errors.Wrapf(err, "field %q", f.GetName())
		}
		columns[f.GetName()] = read
	}
	for _, f := range fields {
		if columns[f.GetName()] == nil && !common.IsSystemField(f.GetFieldID()) && !f.GetIsFunctionOutput() && !f.GetNullable() && !f.GetIsDynamic() && f.GetDefaultValue() == nil {
			return errors.Newf("required field %q is missing from WAL insert", f.GetName())
		}
	}
	for _, sf := range schema.GetStructArrayFields() {
		children := make(map[string]readValue)
		for _, f := range sf.GetFields() {
			read := columns[f.GetName()]
			if read == nil {
				return errors.Newf("missing struct subfield %q", f.GetName())
			}
			name, err := typeutil.ExtractStructFieldName(f.GetName())
			if err != nil {
				return err
			}
			children[name] = read
			delete(columns, f.GetName())
		}
		columns[sf.GetName()] = structReader(children)
	}
	row := make(map[string]any, len(columns))
	enc := json.NewEncoder(w)
	for i := range int(body.GetNumRows()) {
		for name, read := range columns {
			value, err := read(i)
			if err != nil {
				return errors.Wrapf(err, "field %q row %d", name, i)
			}
			row[name] = value
		}
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	return nil
}

type readValue func(int) (any, error)

// Each flattened subfield contributes one array per Milvus row. Transpose the
// arrays into the list of objects expected by the JSON importer.
func structReader(children map[string]readValue) readValue {
	return func(row int) (any, error) {
		var elements []map[string]any
		nulls := 0
		for name, read := range children {
			value, err := read(row)
			if err != nil {
				return nil, err
			}
			if value == nil {
				nulls++
				continue
			}
			values := reflect.ValueOf(value)
			if values.Kind() != reflect.Slice {
				return nil, errors.Newf("expected array for subfield %q", name)
			}
			if elements == nil {
				elements = make([]map[string]any, values.Len())
				for i := range elements {
					elements[i] = make(map[string]any, len(children))
				}
			}
			if values.Len() != len(elements) {
				return nil, errors.New("inconsistent struct subfield lengths")
			}
			for i := range elements {
				elements[i][name] = values.Index(i).Interface()
			}
		}
		if nulls != 0 && nulls != len(children) {
			return nil, errors.New("inconsistent struct subfield validity")
		}
		return elements, nil
	}
}

func columnReader(fd *schemapb.FieldData, rows int) (readValue, error) {
	n, get, err := physicalColumn(fd)
	if err != nil {
		return nil, err
	}
	valid := fd.GetValidData()
	if len(valid) == 0 {
		if n != rows {
			return nil, errors.Newf("got %d values for %d rows", n, rows)
		}
		return get, nil
	}
	if len(valid) != rows {
		return nil, errors.Newf("validity bitmap has %d entries for %d rows", len(valid), rows)
	}
	// WAL scalars can be either padded or compacted, depending on the producer.
	indices, count := typeutil.BuildNullableVectorDataIndices(valid)
	if n != rows && n != count {
		return nil, errors.Newf("got %d values for %d rows (%d valid)", n, rows, count)
	}
	return func(i int) (any, error) {
		if !valid[i] {
			return nil, nil
		}
		if n != rows {
			i = indices[i]
		}
		return get(i)
	}, nil
}

func scalarColumn[T any](v []T) (int, readValue, error) {
	return len(v), func(i int) (any, error) { return v[i], nil }, nil
}

func physicalColumn(fd *schemapb.FieldData) (int, readValue, error) {
	s := fd.GetScalars()
	switch fd.GetType() {
	case schemapb.DataType_Bool:
		return scalarColumn(s.GetBoolData().GetData())
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return scalarColumn(s.GetIntData().GetData())
	case schemapb.DataType_Int64:
		return scalarColumn(s.GetLongData().GetData())
	case schemapb.DataType_Float:
		return scalarColumn(s.GetFloatData().GetData())
	case schemapb.DataType_Double:
		return scalarColumn(s.GetDoubleData().GetData())
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		return scalarColumn(s.GetStringData().GetData())
	case schemapb.DataType_JSON:
		data := s.GetJsonData().GetData()
		return len(data), func(i int) (any, error) {
			if !json.Valid(data[i]) {
				return nil, errors.New("invalid JSON payload")
			}
			// Objects pass through the importer's UseNumber decoder, including $meta.
			if bytes.TrimSpace(data[i])[0] == '{' {
				return json.RawMessage(data[i]), nil
			}
			// The importer accepts other JSON roots as serialized JSON strings.
			return string(data[i]), nil
		}, nil
	case schemapb.DataType_Timestamptz:
		data := s.GetTimestamptzData().GetData()
		return len(data), func(i int) (any, error) { return time.UnixMicro(data[i]).UTC().Format(time.RFC3339Nano), nil }, nil
	case schemapb.DataType_Geometry:
		data := s.GetGeometryData().GetData()
		return len(data), func(i int) (any, error) { return common.ConvertWKBToWKT(data[i]) }, nil
	case schemapb.DataType_Array:
		data := s.GetArrayData().GetData()
		return len(data), func(i int) (any, error) { return arrayValue(data[i]) }, nil
	case schemapb.DataType_ArrayOfVector:
		a := fd.GetVectors().GetVectorArray()
		return len(a.GetData()), func(i int) (any, error) {
			n, get, err := vectorColumn(a.GetElementType(), a.GetData()[i])
			if err != nil {
				return nil, err
			}
			v := make([]any, n)
			for j := range v {
				v[j], err = get(j)
				if err != nil {
					return nil, err
				}
			}
			return v, nil
		}, nil
	default:
		return vectorColumn(fd.GetType(), fd.GetVectors())
	}
}

func vectorColumn(t schemapb.DataType, v *schemapb.VectorField) (int, readValue, error) {
	if v.GetDim() > math.MaxInt/2 {
		return 0, nil, errors.New("vector dimension too large")
	}
	n, err := funcutil.GetVectorFieldPhysicalRows("", t, v)
	if err != nil {
		return 0, nil, err
	}
	get := typeutil.GetDataIterator(&schemapb.FieldData{Type: t, Field: &schemapb.FieldData_Vectors{Vectors: v}})
	return int(n), func(i int) (any, error) {
		value := get(i)
		switch t {
		case schemapb.DataType_BinaryVector:
			return numericBytes[uint16](value.([]byte)), nil
		case schemapb.DataType_Int8Vector:
			return numericBytes[int8](value.([]byte)), nil
		case schemapb.DataType_Float16Vector:
			return typeutil.Float16BytesToFloat32Vector(value.([]byte)), nil
		case schemapb.DataType_BFloat16Vector:
			return typeutil.BFloat16BytesToFloat32Vector(value.([]byte)), nil
		case schemapb.DataType_SparseFloatVector:
			data := value.([]byte)
			if err := typeutil.ValidateSparseFloatRows(data); err != nil {
				return nil, err
			}
			sparse := make(map[uint32]float32)
			for j := range typeutil.SparseFloatRowElementCount(data) {
				sparse[typeutil.SparseFloatRowIndexAt(data, j)] = typeutil.SparseFloatRowValueAt(data, j)
			}
			return sparse, nil
		default:
			return value, nil
		}
	}, nil
}

func numericBytes[T uint16 | int8](v []byte) []T {
	out := make([]T, len(v))
	for i, b := range v {
		out[i] = T(b)
	}
	return out
}

func arrayValue(v *schemapb.ScalarField) (any, error) {
	// An empty repeated field decodes as nil; it represents [] in an import row.
	switch a := v.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		return arraySlice(a.BoolData.GetData()), nil
	case *schemapb.ScalarField_IntData:
		return arraySlice(a.IntData.GetData()), nil
	case *schemapb.ScalarField_LongData:
		return arraySlice(a.LongData.GetData()), nil
	case *schemapb.ScalarField_FloatData:
		return arraySlice(a.FloatData.GetData()), nil
	case *schemapb.ScalarField_DoubleData:
		return arraySlice(a.DoubleData.GetData()), nil
	case *schemapb.ScalarField_StringData:
		return arraySlice(a.StringData.GetData()), nil
	default:
		return nil, errors.Newf("unsupported array element payload %T", a)
	}
}

func arraySlice[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}
