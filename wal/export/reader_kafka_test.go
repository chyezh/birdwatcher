//go:build WKAFKA

package export

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
)

func TestExportKafkaOffsets(t *testing.T) {
	broker, err := kafka.NewMockCluster(1)
	require.NoError(t, err)
	defer broker.Close()
	addr := broker.BootstrapServers()
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": addr})
	require.NoError(t, err)
	defer p.Close()
	topic := "wal-export"
	deliveries := make(chan kafka.Event, 4)
	for i, seg := range []int64{7, 7, 99, 7} {
		require.NoError(t, p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}, Value: legacyRecord(t, seg, int64(i))}, deliveries))
	}
	for range 4 {
		select {
		case report := <-deliveries:
			require.NoError(t, report.(*kafka.Message).TopicPartition.Error)
		case <-time.After(8 * time.Second):
			t.Fatal("fixture delivery timed out")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var got []int64
	err = Read(ctx, "kafka", topic, addr, "1", map[int64]uint64{7: 0}, func(_ int64, b *msgpb.InsertRequest) error { got = append(got, b.RowIDs...); return nil })
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3}, got)
	require.ErrorContains(t, Read(ctx, "kafka", topic, addr, "4", nil, nil), "outside retained range")
	err = Read(ctx, "kafka", topic, addr, "1", map[int64]uint64{7: 0}, func(int64, *msgpb.InsertRequest) error { return context.Canceled })
	require.ErrorIs(t, err, context.Canceled)
	cancel()
	require.ErrorIs(t, Read(ctx, "kafka", topic, addr, "1", nil, nil), context.Canceled)
}
