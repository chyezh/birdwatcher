package export

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestImportJSONValues(t *testing.T) {
	sparse, err := entity.NewSliceSparseEmbedding([]uint32{3}, []float32{1.5})
	require.NoError(t, err)
	cases := []struct {
		c    column.Column
		json string
	}{
		{column.NewColumnBool("v", []bool{true}), "true"},
		{column.NewColumnInt32("v", []int32{-3}), "-3"},
		{column.NewColumnInt64("v", []int64{9007199254740993}), "9007199254740993"},
		{column.NewColumnFloat("v", []float32{1.25}), "1.25"},
		{column.NewColumnDouble("v", []float64{2.5}), "2.5"},
		{column.NewColumnVarChar("v", []string{"é"}), `"é"`},
		{column.NewColumnJSONBytes("v", [][]byte{[]byte(`[1,"x"]`)}), `"[1,\"x\"]"`},
		{column.NewColumnFloatVector("v", 2, [][]float32{{0, 1.5}}), "[0,1.5]"},
		{column.NewColumnFloat16Vector("v", 1, [][]byte{{0, 60}}), "[1]"},
		{column.NewColumnBFloat16Vector("v", 1, [][]byte{{128, 63}}), "[1]"},
		{column.NewColumnBinaryVector("v", 16, [][]byte{{255, 128}}), "[255,128]"},
		{column.NewColumnInt8Vector("v", 2, [][]int8{{-1, 127}}), "[-1,127]"},
		{column.NewColumnSparseVectors("v", []entity.SparseEmbedding{sparse}), `{"3":1.5}`},
		{column.NewColumnBoolArray("v", [][]bool{{true}}), "[true]"},
		{column.NewColumnInt32Array("v", [][]int32{{1}}), "[1]"},
		{column.NewColumnInt64Array("v", [][]int64{{-1}}), "[-1]"},
		{column.NewColumnFloatArray("v", [][]float32{{2.5}}), "[2.5]"},
		{column.NewColumnDoubleArray("v", [][]float64{{3.5}}), "[3.5]"},
		{column.NewColumnVarCharArray("v", [][]string{nil}), "[]"},
		{column.NewColumnFloatVectorArray("v", 2, [][][]float32{{{1, 2}, {3, 4}}}), "[[1,2],[3,4]]"},
	}
	for _, tc := range cases {
		t.Run(tc.c.Type().String(), func(t *testing.T) {
			fd := tc.c.FieldData()
			read, err := columnReader(fd, 1)
			require.NoError(t, err)
			val, err := read(0)
			require.NoError(t, err)
			result, err := json.Marshal(val)
			require.NoError(t, err)
			require.Equal(t, tc.json, string(result))
		})
	}
	wkb, err := common.ConvertWKTToWKB("POINT (1 2)")
	require.NoError(t, err)
	for _, tc := range []struct {
		dt       schemapb.DataType
		data     *schemapb.ScalarField
		expected any
	}{
		{schemapb.DataType_Geometry, &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: [][]byte{wkb}}}}, "POINT (1 2)"},
		{schemapb.DataType_Timestamptz, &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1}}}}, "1970-01-01T00:00:00.000001Z"},
	} {
		fd := &schemapb.FieldData{Type: tc.dt, Field: &schemapb.FieldData_Scalars{Scalars: tc.data}}
		read, err := columnReader(fd, 1)
		require.NoError(t, err)
		v, err := read(0)
		require.NoError(t, err)
		require.Equal(t, tc.expected, v)
	}
}

func TestWriteInsert(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		{FieldID: 101, Name: "v", DataType: schemapb.DataType_VarChar, Nullable: true},
		{FieldID: 102, Name: "generated", DataType: schemapb.DataType_Int64, IsFunctionOutput: true},
	}, StructArrayFields: []*schemapb.StructArrayFieldSchema{{Name: "objects", Fields: []*schemapb.FieldSchema{
		{FieldID: 103, Name: "objects[k]", DataType: schemapb.DataType_Array},
		{FieldID: 104, Name: "objects[v]", DataType: schemapb.DataType_ArrayOfVector},
	}}}}
	for _, values := range [][]string{{"one", "three"}, {"one", "unused", "three"}} {
		var out bytes.Buffer
		fd := column.NewColumnVarChar("v", values).FieldData()
		fd.ValidData = []bool{true, false, true}
		body := &msgpb.InsertRequest{NumRows: 3, FieldsData: []*schemapb.FieldData{
			column.NewColumnInt64("id", []int64{1, 2, 3}).FieldData(), fd, column.NewColumnInt64("generated", []int64{1, 2, 3}).FieldData(),
			column.NewColumnInt32Array("objects[k]", [][]int32{{1}, nil, {3}}).FieldData(),
			column.NewColumnFloatVectorArray("objects[v]", 2, [][][]float32{{{1, 2}}, {}, {{3, 4}}}).FieldData(),
		}}
		for i, field := range body.FieldsData {
			field.FieldId = int64(100 + i)
		}
		body.FieldsData = append(body.FieldsData, &schemapb.FieldData{FieldId: 1})
		require.NoError(t, WriteInsert(&out, schema, body))
		require.Equal(t, "{\"id\":1,\"objects\":[{\"k\":1,\"v\":[1,2]}],\"v\":\"one\"}\n{\"id\":2,\"objects\":[],\"v\":null}\n{\"id\":3,\"objects\":[{\"k\":3,\"v\":[3,4]}],\"v\":\"three\"}\n", out.String())
	}
}

func TestInvalidInsert(t *testing.T) {
	fd := column.NewColumnInt64("id", []int64{1}).FieldData()
	fd.FieldId = 100
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: fd.Type}}}
	for _, body := range []*msgpb.InsertRequest{{}, {NumRows: 2, FieldsData: []*schemapb.FieldData{fd}}, {NumRows: 1, FieldsData: []*schemapb.FieldData{fd, fd}}, {NumRows: 1, FieldsData: []*schemapb.FieldData{{FieldId: 101}}}, {NumRows: 1, FieldsData: []*schemapb.FieldData{{FieldId: 0}}}} {
		require.Error(t, WriteInsert(io.Discard, schema, body))
	}
	fd.ValidData = []bool{true, true}
	_, err := columnReader(fd, 1)
	require.Error(t, err)
	_, err = columnReader(fd, 2)
	require.Error(t, err)
	for _, f := range []*schemapb.FieldData{column.NewColumnJSONBytes("id", [][]byte{{'{'}}).FieldData(), column.NewColumnDouble("id", []float64{math.Inf(1)}).FieldData()} {
		f.FieldId = 100
		schema.Fields[0].DataType = f.Type
		require.Error(t, WriteInsert(io.Discard, schema, &msgpb.InsertRequest{NumRows: 1, FieldsData: []*schemapb.FieldData{f}}))
	}
}

func TestInvalidStructData(t *testing.T) {
	constant := func(v any) readValue { return func(int) (any, error) { return v, nil } }
	for _, tc := range []map[string]readValue{
		{"k": constant(nil), "v": constant([]int{})},
		{"k": constant([]int{1}), "v": constant([]int{})},
		{"k": constant("x")},
		{"k": func(int) (any, error) { return nil, io.ErrUnexpectedEOF }},
	} {
		_, err := structReader(tc)(0)
		require.Error(t, err)
	}
	v, err := structReader(map[string]readValue{"k": constant(nil)})(0)
	require.NoError(t, err)
	require.Nil(t, v)
	_, err = arrayValue(&schemapb.ScalarField{})
	require.Error(t, err)
	_, _, err = vectorColumn(schemapb.DataType_FloatVector, &schemapb.VectorField{Dim: math.MaxInt64})
	require.Error(t, err)
	_, _, err = vectorColumn(schemapb.DataType_FloatVector, nil)
	require.Error(t, err)
	fd := column.NewColumnJSONBytes("meta", [][]byte{[]byte(`{"i":9007199254740993}`)}).FieldData()
	read, err := columnReader(fd, 1)
	require.NoError(t, err)
	val, err := read(0)
	require.NoError(t, err)
	require.Equal(t, json.RawMessage(`{"i":9007199254740993}`), val)
}
