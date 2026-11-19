//go:build WKAFKA

package export

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	kafkaconfig "github.com/milvus-io/milvus/pkg/v3/mq/msgstream/mqwrapper/kafka"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	walkafka "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/kafka"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func init() { readKafkaExport = readKafkaPrefix }

func readKafkaPrefix(ctx context.Context, topic, addr string, start string, consume func(message.ImmutableMessage) error) error {
	offset, err := strconv.ParseInt(start, 10, 64)
	if err != nil || offset < 0 {
		return errors.Newf("invalid Kafka offset %q", start)
	}
	cfg := &paramtable.Get().KafkaCfg
	opts := kafkaconfig.GetBasicConfig(cfg)
	for k, v := range cfg.ConsumerExtraConfig.GetValue() {
		opts[k] = v
	}
	if addr != "" {
		opts["bootstrap.servers"] = addr
	}
	opts["group.id"] = fmt.Sprintf("birdwatcher-export-%d", time.Now().UnixNano())
	opts["enable.auto.commit"] = false
	opts["auto.offset.reset"] = "error"
	opts["enable.partition.eof"] = true
	c, err := kafka.NewConsumer(&opts)
	if err != nil {
		return err
	}
	defer c.Close()
	low, high, err := c.QueryWatermarkOffsets(topic, 0, 10000)
	if err != nil {
		return err
	}

	if offset < low || offset >= high {
		return errors.Newf("offset %d outside retained range [%d,%d)", offset, low, high)
	}
	if err = c.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: 0, Offset: kafka.Offset(offset)}}); err != nil {
		return err
	}
	for ctx.Err() == nil {
		switch e := c.Poll(200).(type) {
		case kafka.Error:
			return e
		case kafka.PartitionEOF:
			if int64(e.Offset) >= high {
				return nil
			}
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				return e.TopicPartition.Error
			}
			if int64(e.TopicPartition.Offset) >= high {
				return nil
			}
			props := make(map[string]string, len(e.Headers))
			for _, h := range e.Headers {
				props[h.Key] = string(h.Value)
			}
			if err := consume(message.NewImmutableMesasge(walkafka.NewKafkaID(e.TopicPartition.Offset), e.Value, props)); err != nil {
				return err
			}
			if int64(e.TopicPartition.Offset) == high-1 {
				return nil
			}
		}
	}
	return ctx.Err()
}
