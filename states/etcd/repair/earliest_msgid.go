//go:build !WKAFKA
// +build !WKAFKA

package repair

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	pulsarimpl "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	wpimpl "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/wp"
)

// buildEarliestMessageID returns the earliest message.MessageID for the given MQ type.
// kafka and rocksmq are not available without WKAFKA build tag (CGO required).
func buildEarliestMessageID(mqType string) (message.MessageID, error) {
	switch mqType {
	case "pulsar":
		return pulsarimpl.NewPulsarID(pulsar.EarliestMessageID()), nil
	case "woodpecker":
		earliest := wplog.EarliestLogMessageID()
		return wpimpl.NewWpID(&earliest), nil
	default:
		return nil, fmt.Errorf("unsupported mq_type %q (build without WKAFKA tag, kafka and rocksmq not available)", mqType)
	}
}

// buildEarliestMsgIDBytes returns the earliest message ID as []byte for DataCoord checkpoints.
func buildEarliestMsgIDBytes(mqType string) ([]byte, error) {
	switch mqType {
	case "pulsar":
		return pulsar.EarliestMessageID().Serialize(), nil
	case "rocksmq":
		// RocksMQ earliest ID is 0, serialized as 8-byte little-endian int64.
		return make([]byte, 8), nil
	case "woodpecker":
		earliest := wplog.EarliestLogMessageID()
		return earliest.Serialize(), nil
	default:
		return nil, fmt.Errorf("unsupported mq_type %q (build without WKAFKA tag, kafka not available)", mqType)
	}
}
