//go:build WKAFKA
// +build WKAFKA

package repair

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	kafkaimpl "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/kafka"
	pulsarimpl "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	rmqimpl "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	wpimpl "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/wp"
)

// buildEarliestMessageID returns the earliest message.MessageID for the given MQ type.
func buildEarliestMessageID(mqType string) (message.MessageID, error) {
	switch mqType {
	case "pulsar":
		return pulsarimpl.NewPulsarID(pulsar.EarliestMessageID()), nil
	case "kafka":
		return kafkaimpl.NewKafkaID(0), nil
	case "rocksmq":
		return rmqimpl.NewRmqID(0), nil
	case "woodpecker":
		earliest := wplog.EarliestLogMessageID()
		return wpimpl.NewWpID(&earliest), nil
	default:
		return nil, fmt.Errorf("unsupported mq_type %q", mqType)
	}
}

// buildEarliestMsgIDBytes returns the earliest message ID as []byte for DataCoord checkpoints.
func buildEarliestMsgIDBytes(mqType string) ([]byte, error) {
	switch mqType {
	case "pulsar":
		return pulsar.EarliestMessageID().Serialize(), nil
	case "kafka":
		return make([]byte, 8), nil
	case "rocksmq":
		return make([]byte, 8), nil
	case "woodpecker":
		earliest := wplog.EarliestLogMessageID()
		return earliest.Serialize(), nil
	default:
		return nil, fmt.Errorf("unsupported mq_type %q", mqType)
	}
}
