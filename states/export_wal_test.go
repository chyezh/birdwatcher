package states

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestExportWALInvalidArguments(t *testing.T) {
	dir := t.TempDir()
	s := &InstanceState{}
	require.Error(t, s.ExportWALCommand(t.Context(), &ExportWALParams{}))
	p := &ExportWALParams{WAL: "pulsar", Topic: "p", Start: "bad!", Output: filepath.Join(dir, "export"), Segments: []int64{-1}}
	require.ErrorContains(t, s.ExportWALCommand(t.Context(), p), "segment ID")
	p.Segments = []int64{1}
	require.Error(t, s.ExportWALCommand(t.Context(), p))
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
	p.Output = dir
	require.ErrorContains(t, s.ExportWALCommand(t.Context(), p), "already exists")
	p.Output = filepath.Join(dir, "missing", "output")
	require.Error(t, s.ExportWALCommand(t.Context(), p))
	require.Error(t, appendWALInsert(dir, nil, nil))
	require.Error(t, appendWALInsert(filepath.Join(dir, "test.jsonl"), &schemapb.CollectionSchema{}, &msgpb.InsertRequest{}))
}
