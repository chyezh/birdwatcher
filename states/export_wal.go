package states

import (
	"bufio"
	"context"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/wal/export"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

type ExportWALParams struct {
	framework.ParamBase `use:"export-wal" desc:"export committed WAL inserts for selected segments as Milvus import JSONL"`
	Segments            []int64 `name:"segments" desc:"comma-separated segment IDs"`
	Topic               string  `name:"topic" desc:"physical channel name"`
	Start               string  `name:"message-id" desc:"inclusive start: Pulsar ledger/entry/batch, WAL base64 ID or earliest, Kafka decimal offset"`
	WAL                 string  `name:"wal" default:"pulsar" desc:"WAL backend: pulsar or kafka (WKAFKA build)"`
	Address             string  `name:"mq-addr" desc:"broker host:port (defaults to configured broker)"`
	Output              string  `name:"output" default:"wal-export" desc:"new directory for segment JSONL files"`
}

func (s *InstanceState) ExportWALCommand(ctx context.Context, p *ExportWALParams) error {
	if len(p.Segments) == 0 || p.Topic == "" || p.Start == "" || p.Output == "" {
		return errors.New("provide --segments, --topic, --message-id, a new output directory")
	}
	counts := make(map[int64]uint64)
	for _, id := range p.Segments {
		if id <= 0 {
			return errors.Newf("invalid segment ID %d", id)
		}
		counts[id] = 0
	}
	dest := filepath.Clean(p.Output)
	if _, err := os.Lstat(dest); err == nil {
		return errors.Newf("output %q already exists", dest)
	} else if !os.IsNotExist(err) {
		return err
	}
	stage, err := os.MkdirTemp(filepath.Dir(dest), ".wal-export-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	schemas := make(map[int64]*schemapb.CollectionSchema)
	write := func(segment int64, body *msgpb.InsertRequest) error {
		schema := schemas[body.GetCollectionID()]
		if schema == nil {
			coll, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, body.GetCollectionID())
			if err != nil {
				return err
			}
			schema = coll.GetProto().GetSchema()
			schemas[body.GetCollectionID()] = schema
		}
		path := filepath.Join(stage, fmt.Sprintf("%d.jsonl", segment))
		if err := appendWALInsert(path, schema, body); err != nil {
			return err
		}
		counts[segment] += body.GetNumRows()
		return nil
	}
	if err := export.Read(ctx, p.WAL, p.Topic, p.Address, p.Start, counts, write); err != nil {
		return err
	}
	if err := os.Rename(stage, dest); err != nil {
		return err
	}
	for _, id := range slices.Sorted(maps.Keys(counts)) {
		fmt.Printf("segment %d: %d rows\n", id, counts[id])
	}
	return nil
}

func appendWALInsert(path string, schema *schemapb.CollectionSchema, body *msgpb.InsertRequest) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	buf := bufio.NewWriter(f)
	err = export.WriteInsert(buf, schema, body)
	if err == nil {
		err = buf.Flush()
	}
	return errors.CombineErrors(err, f.Close())
}
