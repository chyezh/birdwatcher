package export

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	walpulsar "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
)

func legacyRecord(t *testing.T, seg, pk int64) []byte {
	t.Helper()
	data, err := proto.Marshal(&msgpb.InsertRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert}, SegmentID: seg, RowIDs: []int64{pk}})
	require.NoError(t, err)
	return data
}

func TestReplayTransactions(t *testing.T) {
	const seg = 123
	insert := func(pk int64) message.MutableMessage {
		m, err := message.NewInsertMessageBuilderV1().WithVChannel("v").WithHeader(&message.InsertMessageHeader{CollectionId: 5, Partitions: []*message.PartitionSegmentAssignment{
			{PartitionId: 6, Rows: 1, SegmentAssignment: &message.SegmentAssignment{SegmentId: seg}},
		}}).WithBody(&msgpb.InsertRequest{CollectionID: 5, PartitionID: 6, SegmentID: 999, NumRows: 1, RowIDs: []int64{pk}}).BuildMutable()
		require.NoError(t, err)
		return m
	}
	txn := func(id message.TxnID, tick uint64, m message.MutableMessage) message.ImmutableMessage {
		mid := walpulsar.NewPulsarID(pulsar.NewMessageID(1, int64(tick), -1, -1))
		return m.WithTimeTick(tick).WithLastConfirmed(mid).WithTxnContext(message.TxnContext{TxnID: id, Keepalive: time.Second}).IntoImmutableMessage(mid)
	}
	begin := func() message.MutableMessage {
		m, err := message.NewBeginTxnMessageBuilderV2().WithVChannel("v").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).BuildMutable()
		require.NoError(t, err)
		return m
	}
	commit := func() message.MutableMessage {
		m, err := message.NewCommitTxnMessageBuilderV2().WithVChannel("v").WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).BuildMutable()
		require.NoError(t, err)
		return m
	}
	rollback, err := message.NewRollbackTxnMessageBuilderV2().WithVChannel("v").WithHeader(&message.RollbackTxnMessageHeader{}).WithBody(&message.RollbackTxnMessageBody{}).BuildMutable()
	require.NoError(t, err)
	id := walpulsar.NewPulsarID(pulsar.NewMessageID(1, 0, -1, -1))
	entries := []message.ImmutableMessage{
		message.NewImmutableMesasge(id, legacyRecord(t, seg, 1), nil),
		message.NewImmutableMesasge(id, legacyRecord(t, 99, 99), nil),
		insert(2).WithTimeTick(1).IntoImmutableMessage(id),
		txn(1, 2, begin()), txn(1, 4, insert(4)), txn(1, 3, insert(3)), txn(1, 5, commit()),
		txn(2, 6, begin()), txn(2, 7, insert(5)), txn(2, 8, rollback),
		txn(3, 9, begin()), txn(3, 10, insert(6)),
		txn(4, 11, insert(7)), txn(4, 12, commit()),
	}
	var got []int64
	err = replay("topic", map[int64]uint64{seg: 0}, func(id int64, b *msgpb.InsertRequest) error {
		require.EqualValues(t, seg, id)
		got = append(got, b.RowIDs...)
		return nil
	},
		func(visit func(message.ImmutableMessage) error) error {
			for _, m := range entries {
				if err := visit(m); err != nil {
					return err
				}
			}
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, got)
}

func TestExportPositions(t *testing.T) {
	id, err := pulsarStartID("42/10/-1")
	require.NoError(t, err)
	bin := base64.StdEncoding.EncodeToString(id.Serialize())
	id2, err := pulsarStartID(bin)
	require.NoError(t, err)
	require.Equal(t, id.Serialize(), id2.Serialize())
	_, err = pulsarStartID("invalid!")
	require.Error(t, err)
	require.Error(t, Read(t.Context(), "bogus", "topic", "", "", nil, nil))
	require.Error(t, Read(t.Context(), "kafka", "topic", "", "-1", nil, nil))
}

// Set BIRDWATCHER_TEST_PULSAR to exercise a real ledger, including a start
// inside a batch and an append after the reader has captured its end position.
func TestExportPulsarBatch(t *testing.T) {
	addr := os.Getenv("BIRDWATCHER_TEST_PULSAR")
	if addr == "" {
		t.Skip("requires a Pulsar broker")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: addr})
	require.NoError(t, err)
	defer client.Close()
	topic := fmt.Sprintf("bw-wal-export-test-%d", time.Now().UnixNano())
	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topic, BatchingMaxPublishDelay: time.Second})
	require.NoError(t, err)
	defer producer.Close()
	ids := make([]pulsar.MessageID, 3)
	sendErrs := make([]error, 3)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		producer.SendAsync(ctx, &pulsar.ProducerMessage{Payload: legacyRecord(t, 7, int64(i))}, func(id pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
			ids[i] = id
			sendErrs[i] = err
			wg.Done()
		})
	}
	require.NoError(t, producer.Flush())
	wg.Wait()
	for _, err := range sendErrs {
		require.NoError(t, err)
	}
	require.Equal(t, ids[0].EntryID(), ids[2].EntryID())
	require.EqualValues(t, 1, ids[1].BatchIdx())
	var got []int64
	err = Read(ctx, "pulsar", topic, addr, walpulsar.NewPulsarID(ids[1]).String(), map[int64]uint64{7: 0}, func(_ int64, b *msgpb.InsertRequest) error {
		if len(got) == 0 {
			_, err := producer.Send(ctx, &pulsar.ProducerMessage{Payload: legacyRecord(t, 7, 99)})
			if err != nil {
				return err
			}
		}
		got = append(got, b.RowIDs...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, got)
	require.ErrorContains(t, Read(ctx, "pulsar", topic, addr, fmt.Sprintf("%d/%d/-1", ids[0].LedgerID(), ids[0].EntryID()+100), nil, nil), "beyond topic tail")
	err = Read(ctx, "pulsar", topic, addr, "earliest", map[int64]uint64{7: 0}, func(int64, *msgpb.InsertRequest) error { return context.Canceled })
	require.ErrorIs(t, err, context.Canceled)
}

func TestReplayFailure(t *testing.T) {
	id := walpulsar.NewPulsarID(pulsar.NewMessageID(1, 0, -1, -1))
	insert, err := message.NewInsertMessageBuilderV1().WithVChannel("v").WithHeader(&message.InsertMessageHeader{Partitions: []*message.PartitionSegmentAssignment{{Rows: 1, SegmentAssignment: &message.SegmentAssignment{SegmentId: 7}}}}).WithBody(&msgpb.InsertRequest{}).BuildMutable()
	require.NoError(t, err)
	msg := insert.WithTimeTick(2).IntoImmutableMessage(id)
	require.NoError(t, visitWALInserts(msg, nil, nil))
	require.ErrorContains(t, visitWALInserts(msg, map[int64]uint64{7: 0}, nil), "differs")
	raw := msg.IntoMessageProto()
	raw.Properties["_ch"] = "encrypted"
	require.ErrorContains(t, visitWALInserts(message.NewImmutableMesasge(id, nil, raw.Properties), map[int64]uint64{7: 0}, nil), "encrypted")
	delete(raw.Properties, "_ch")
	require.Error(t, visitWALInserts(message.NewImmutableMesasge(id, []byte{0xff}, raw.Properties), map[int64]uint64{7: 0}, nil))
	delete(raw.Properties, "_h")
	require.Error(t, visitWALInserts(message.NewImmutableMesasge(id, nil, raw.Properties), map[int64]uint64{7: 0}, nil))
	err = replay("t", nil, nil, func(visit func(message.ImmutableMessage) error) error {
		return visit(message.NewImmutableMesasge(id, []byte{0xff}, nil))
	})
	require.Error(t, err)
}

func TestReplayTimeTickAndScanErrors(t *testing.T) {
	id := walpulsar.NewPulsarID(pulsar.NewMessageID(1, 1, -1, -1))
	tick, err := message.NewTimeTickMessageBuilderV1().WithAllVChannel().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).BuildMutable()
	require.NoError(t, err)
	im := tick.WithTimeTick(12).WithLastConfirmed(id).IntoImmutableMessage(id)
	err = replay("topic", nil, nil, func(visit func(message.ImmutableMessage) error) error { return visit(im) })
	require.NoError(t, err)
	err = replay("topic", nil, nil, func(visit func(message.ImmutableMessage) error) error {
		return visit(message.NewImmutableMesasge(id, nil, map[string]string{"_ch": "encrypted"}))
	})
	require.ErrorContains(t, err, "encrypted")
	err = replay("topic", nil, nil, func(visit func(message.ImmutableMessage) error) error {
		return visit(message.NewImmutableMesasge(id, nil, nil))
	})
	require.NoError(t, err)
	// A valid outer header can still carry an invalid nested insert body.
	err = replay("topic", nil, nil, func(visit func(message.ImmutableMessage) error) error {
		payload := append(legacyRecord(t, 1, 2), 0x6a, 0x01, 0xff)
		return visit(message.NewImmutableMesasge(id, payload, nil))
	})
	require.Error(t, err)
}
