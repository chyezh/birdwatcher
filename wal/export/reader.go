package export

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/wal/utility"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	walpulsar "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type walReader func(context.Context, string, string, string, func(message.ImmutableMessage) error) error

var readKafkaExport walReader

// Read consumes a finite, inclusive topic prefix and delivers inserts for selected segments.
func Read(ctx context.Context, wal, topic, addr, start string, segments map[int64]uint64, write func(int64, *msgpb.InsertRequest) error) error {
	var read walReader
	switch strings.ToLower(wal) {
	case "pulsar":
		read = readPulsarExport
	case "kafka":
		if readKafkaExport == nil {
			return errors.New("Kafka requires a WKAFKA build")
		}
		read = readKafkaExport
	default:
		return errors.Newf("unsupported export WAL %q", wal)
	}
	return replay(topic, segments, write, func(visit func(message.ImmutableMessage) error) error { return read(ctx, topic, addr, start, visit) })
}

func replay(topic string, segments map[int64]uint64, write func(int64, *msgpb.InsertRequest) error, scan func(func(message.ImmutableMessage) error) error) error {
	// Reuse the streaming scanner's ordering and transaction assembly. Drain the
	// finite prefix at EOF even if an idle topic never persists another TimeTick.
	reorder := utility.NewReOrderBuffer()
	txns := utility.NewTxnBuffer(mlog.With())
	drain := func(tick uint64) error {
		for _, msg := range txns.HandleImmutableMessages(reorder.PopUtilTimeTick(tick), tick) {
			if err := visitWALInserts(msg, segments, write); err != nil {
				return errors.Wrapf(err, "topic %s message %s", topic, msg.MessageID())
			}
		}
		return nil
	}
	consume := func(msg message.ImmutableMessage) error {
		if msg.Version() == message.VersionOld {
			if msg.Properties().Exist("_ch") {
				return errors.New("cannot decode encrypted legacy WAL record")
			}
			var header commonpb.MsgHeader
			if err := proto.Unmarshal(msg.Payload(), &header); err != nil {
				return err
			}
			if header.GetBase().GetMsgType() != commonpb.MsgType_Insert {
				return nil
			}
			var body msgpb.InsertRequest
			if err := proto.Unmarshal(msg.Payload(), &body); err != nil {
				return err
			}
			if _, ok := segments[body.GetSegmentID()]; ok {
				return write(body.GetSegmentID(), &body)
			}
			return nil
		}
		if msg.MessageType() == message.MessageTypeTimeTick {
			return drain(msg.TimeTick())
		}
		if msg.TxnContext() == nil {
			return visitWALInserts(msg, segments, write)
		}
		return reorder.Push(msg)
	}
	if err := scan(consume); err != nil {
		return err
	}
	return drain(math.MaxUint64)
}

func visitWALInserts(msg message.ImmutableMessage, segments map[int64]uint64, write func(int64, *msgpb.InsertRequest) error) error {
	switch msg.MessageType() {
	case message.MessageTypeTxn:
		txn := message.AsImmutableTxnMessage(msg)
		if txn == nil {
			return errors.New("invalid transaction envelope")
		}
		return txn.RangeOver(func(inner message.ImmutableMessage) error { return visitWALInserts(inner, segments, write) })
	case message.MessageTypeInsert:
		insert, err := message.AsImmutableInsertMessageV1(msg)
		if err != nil {
			return err
		}
		for _, a := range insert.Header().GetPartitions() {
			id := a.GetSegmentAssignment().GetSegmentId()
			if _, ok := segments[id]; !ok {
				continue
			}
			if msg.Properties().Exist("_ch") {
				return errors.New("encrypted WAL inserts require decryption and cannot be exported")
			}
			body, err := insert.Body()
			if err != nil {
				return err
			}
			if body.GetPartitionID() == a.GetPartitionId() {
				if body.GetCollectionID() != insert.Header().GetCollectionId() || body.GetNumRows() != a.GetRows() {
					return errors.Newf("insert body differs from WAL assignment for segment %d", id)
				}
				return write(id, body)
			}
		}
	}
	return nil
}

func pulsarStartID(value string) (pulsar.MessageID, error) {
	if value == "earliest" {
		return pulsar.EarliestMessageID(), nil
	}
	parts := strings.Split(value, "/")
	if len(parts) == 3 {
		ledger, e1 := strconv.ParseInt(parts[0], 10, 64)
		entry, e2 := strconv.ParseInt(parts[1], 10, 64)
		batch, e3 := strconv.ParseInt(parts[2], 10, 32)
		if e1 == nil && e2 == nil && e3 == nil && ledger >= 0 && entry >= 0 && batch >= -1 {
			return pulsar.NewMessageID(ledger, entry, int32(batch), -1), nil
		}
	}
	id, err := walpulsar.UnmarshalMessageID(value)
	if err != nil {
		return nil, err
	}
	return id.(interface{ PulsarID() pulsar.MessageID }).PulsarID(), nil
}

func readPulsarExport(ctx context.Context, topic, addr string, position string, consume func(message.ImmutableMessage) error) error {
	raw, err := pulsarStartID(position)
	if err != nil {
		return err
	}
	start := walpulsar.NewPulsarID(raw)
	cfg := &paramtable.Get().PulsarCfg
	if addr == "" {
		addr = cfg.Address.GetValue()
	} else if !strings.Contains(addr, "://") {
		addr = "pulsar://" + addr
	}
	auth, err := pulsar.NewAuthentication(cfg.AuthPlugin.GetValue(), cfg.AuthParams.GetValue())
	if err != nil {
		return err
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: addr, Authentication: auth, OperationTimeout: cfg.RequestTimeout.GetAsDuration(time.Second)})
	if err != nil {
		return err
	}
	defer client.Close()
	if !strings.Contains(topic, "/") {
		topic = cfg.Tenant.GetValue() + "/" + cfg.Namespace.GetValue() + "/" + topic
	}
	reader, err := client.CreateReader(pulsar.ReaderOptions{Topic: topic, StartMessageID: raw, StartMessageIDInclusive: true})
	if err != nil {
		return err
	}
	defer reader.Close()
	last, err := reader.GetLastMessageID()
	if err != nil {
		return err
	}
	end := walpulsar.NewPulsarID(last)
	atEntry := func(a, b pulsar.MessageID) bool { return a.LedgerID() == b.LedgerID() && a.EntryID() == b.EntryID() }
	if last.EntryID() < 0 {
		return errors.Newf("topic %s is empty", topic)
	}
	tailCoversBatch := atEntry(last, raw) && last.BatchIdx() < 0
	if end.LT(start) && !tailCoversBatch {
		return errors.Newf("start %s is beyond topic tail %s", start, end)
	}
	first := true
	for {
		// The high watermark already exists. A missing/expired batch position
		// must not leave a recovery command waiting forever on an idle topic.
		next, cancel := context.WithTimeout(ctx, time.Minute)
		record, err := reader.Next(next)
		cancel()
		if err != nil {
			return err
		}
		id := walpulsar.NewPulsarID(record.ID())
		if id.LT(start) {
			continue
		}
		startCoversBatch := atEntry(raw, record.ID()) && raw.BatchIdx() < 0
		if first && raw.EntryID() >= 0 && !id.EQ(start) && !startCoversBatch {
			return errors.Newf("requested start %s is unavailable; first record is %s", start, id)
		}
		first = false
		tailEntry := atEntry(record.ID(), last)
		if !tailEntry && end.LT(id) {
			return errors.Newf("topic tail %s was skipped before %s", end, id)
		}
		if err := consume(message.NewImmutableMesasge(id, record.Payload(), record.Properties())); err != nil {
			return err
		}
		// Pulsar publishes a batch in one ledger entry. Some brokers report
		// the tail entry without its last batch index.
		if tailEntry && (record.ID().BatchSize() <= 1 || record.ID().BatchIdx() == record.ID().BatchSize()-1) {
			return nil
		}
	}
}
