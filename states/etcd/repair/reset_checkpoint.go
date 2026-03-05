package repair

import (
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

type ResetCheckpointParam struct {
	framework.ExecutionParam `use:"reset checkpoint" desc:"reset all checkpoint positions to earliest message_id without modifying timetick"`
	MqType                   string `name:"mq_type" default:"kafka" desc:"MQ type for DataCoord channel checkpoints (kafka, pulsar)"`
}

func (c *ComponentRepair) ResetCheckpointCommand(ctx context.Context, p *ResetCheckpointParam) error {
	mqType := strings.ToLower(p.MqType)
	if mqType != "kafka" && mqType != "pulsar" {
		return fmt.Errorf("unsupported mq_type %q, must be kafka or pulsar", p.MqType)
	}

	if err := c.resetDataCoordCheckpoints(ctx, mqType, p.Run); err != nil {
		return err
	}

	if err := c.resetStreamingNodeCheckpoints(ctx, p.Run); err != nil {
		return err
	}

	return nil
}

// resetDataCoordCheckpoints resets all DataCoord channel checkpoints to earliest message_id.
func (c *ComponentRepair) resetDataCoordCheckpoints(ctx context.Context, mqType string, run bool) error {
	checkpoints, err := common.ListChannelCheckpoint(ctx, c.client, c.basePath)
	if err != nil {
		return fmt.Errorf("failed to list channel checkpoints: %w", err)
	}

	if len(checkpoints) == 0 {
		fmt.Println("No DataCoord channel checkpoints found.")
		return nil
	}

	earliestMsgID, err := buildEarliestMsgIDBytes(mqType)
	if err != nil {
		return err
	}

	fmt.Printf("Found %d DataCoord channel checkpoint(s), mq_type=%s\n", len(checkpoints), mqType)
	for _, cp := range checkpoints {
		pos := cp.GetProto()
		channelName := pos.GetChannelName()

		if !run {
			fmt.Printf("[dry-run] would reset DataCoord checkpoint: channel=%s\n", channelName)
			continue
		}

		newPos := &msgpb.MsgPosition{
			ChannelName: pos.GetChannelName(),
			MsgID:       earliestMsgID,
			MsgGroup:    pos.GetMsgGroup(),
			Timestamp:   pos.GetTimestamp(),
		}
		if err := saveChannelCheckpoint(ctx, c.client, c.basePath, channelName, newPos); err != nil {
			return fmt.Errorf("failed to save checkpoint for channel %s: %w", channelName, err)
		}
		fmt.Printf("Reset DataCoord checkpoint: channel=%s\n", channelName)
	}

	return nil
}

// resetStreamingNodeCheckpoints resets all StreamingNode WAL checkpoints to earliest message_id.
func (c *ComponentRepair) resetStreamingNodeCheckpoints(ctx context.Context, run bool) error {
	metas, err := common.ListWALDistribution(ctx, c.client, c.basePath, "")
	if err != nil {
		return fmt.Errorf("failed to list WAL distributions: %w", err)
	}

	if len(metas) == 0 {
		fmt.Println("No StreamingNode WAL distributions found.")
		return nil
	}

	fmt.Printf("Found %d StreamingNode pchannel(s)\n", len(metas))
	for _, meta := range metas {
		pchannel := meta.GetChannel().GetName()
		cpKey := path.Join(c.basePath, common.WALRecoveryStoragePrefix, pchannel, common.WALRecoveryStorageConsumeCheckpoint)

		val, err := c.client.Load(ctx, cpKey)
		if err != nil {
			fmt.Printf("Skipping pchannel=%s: no checkpoint found\n", pchannel)
			continue
		}

		checkpoint := &streamingpb.WALCheckpoint{}
		if err := proto.Unmarshal([]byte(val), checkpoint); err != nil {
			return fmt.Errorf("failed to unmarshal checkpoint for pchannel %s: %w", pchannel, err)
		}

		if checkpoint.GetMessageId() == nil {
			fmt.Printf("Skipping pchannel=%s: checkpoint has no message_id\n", pchannel)
			continue
		}

		walName := checkpoint.GetMessageId().GetWALName()
		earliestID, err := buildEarliestStreamingMsgID(walName)
		if err != nil {
			return fmt.Errorf("failed to build earliest message_id for pchannel %s (wal=%s): %w", pchannel, walName.String(), err)
		}

		if !run {
			fmt.Printf("[dry-run] would reset StreamingNode checkpoint: pchannel=%s wal=%s\n", pchannel, walName.String())
			continue
		}

		checkpoint.MessageId = earliestID
		bs, err := proto.Marshal(checkpoint)
		if err != nil {
			return fmt.Errorf("failed to marshal checkpoint for pchannel %s: %w", pchannel, err)
		}
		if err := c.client.Save(ctx, cpKey, string(bs)); err != nil {
			return fmt.Errorf("failed to save checkpoint for pchannel %s: %w", pchannel, err)
		}
		fmt.Printf("Reset StreamingNode checkpoint: pchannel=%s wal=%s\n", pchannel, walName.String())
	}

	return nil
}

// buildEarliestMsgIDBytes returns the earliest message ID as []byte for DataCoord checkpoints.
func buildEarliestMsgIDBytes(mqType string) ([]byte, error) {
	switch mqType {
	case "pulsar":
		return pulsar.EarliestMessageID().Serialize(), nil
	case "kafka":
		// Kafka earliest offset is 0, encoded as 8-byte little-endian uint64.
		return make([]byte, 8), nil
	default:
		return nil, fmt.Errorf("unsupported mq_type %q", mqType)
	}
}

// buildEarliestStreamingMsgID returns the earliest commonpb.MessageID for StreamingNode checkpoints.
func buildEarliestStreamingMsgID(walName commonpb.WALName) (*commonpb.MessageID, error) {
	switch walName {
	case commonpb.WALName_Pulsar:
		return &commonpb.MessageID{
			Id:      base64.StdEncoding.EncodeToString(pulsar.EarliestMessageID().Serialize()),
			WALName: commonpb.WALName_Pulsar,
		}, nil
	case commonpb.WALName_Kafka:
		return &commonpb.MessageID{
			Id:      message.EncodeUint64(0),
			WALName: commonpb.WALName_Kafka,
		}, nil
	case commonpb.WALName_WoodPecker:
		earliest := wplog.EarliestLogMessageID()
		return &commonpb.MessageID{
			Id:      base64.StdEncoding.EncodeToString(earliest.Serialize()),
			WALName: commonpb.WALName_WoodPecker,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported WAL type %s", walName.String())
	}
}
