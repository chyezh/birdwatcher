package repair

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

var (
	repairBroadcastTaskModeReset  string = "reset"
	repairBroadcastTaskModeRemove string = "remove"
)

type WALBroadcastTaskParam struct {
	framework.ExecutionParam `use:"repair wal-broadcast-task" desc:"repair wal broadcast task"`
	Mode                     string `name:"mode" default:"reset" desc:"reset or remove wal broadcast task"`
	BroadcastID              int64  `name:"broadcast_id" default:"" desc:"broadcast id to repair"`
	MessageType              string `name:"message_type" default:"" desc:"repair all ack-incomplete broadcast tasks of the given message type, e.g. DropCollection"`
}

func (c *ComponentRepair) WALBroadcastTaskCommand(ctx context.Context, p *WALBroadcastTaskParam) error {
	if p.Mode != repairBroadcastTaskModeReset && p.Mode != repairBroadcastTaskModeRemove {
		return fmt.Errorf("invalid mode: %s", p.Mode)
	}

	if p.MessageType != "" {
		return c.repairWalBroadcastTaskByMessageType(ctx, p)
	}

	meta, err := common.ListWalBroadcastByID(ctx, c.client, c.basePath, p.BroadcastID)
	if err != nil {
		if errors.Is(err, common.ErrBroadcastTaskNotFound) {
			fmt.Printf("broadcast task not found with broadcast ID %d\n", p.BroadcastID)
			return nil
		}
		return errors.Wrap(err, "failed to list wal broadcast task")
	}

	detail, err := protojson.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "failed to marshal broadcast task")
	}
	fmt.Printf("Broadcast Task Detail: \n%s\n", string(detail))

	if !p.Run {
		return nil
	}

	switch p.Mode {
	case repairBroadcastTaskModeReset:
		msg := message.NewBroadcastMutableMessageBeforeAppend(meta.Message.Payload, meta.Message.Properties)
		meta.AckedCheckpoints = make([]*streamingpb.AckedCheckpoint, len(msg.BroadcastHeader().VChannels))
		meta.AckedVchannelBitmap = make([]byte, len(msg.BroadcastHeader().VChannels))
		meta.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING
		if err := common.SaveWalBroadcastTask(ctx, c.client, c.basePath, p.BroadcastID, meta); err != nil {
			return errors.Wrap(err, "failed to save wal broadcast task")
		}
		fmt.Printf("wal broadcast task reseted with broadcast ID %d\n", p.BroadcastID)
	case repairBroadcastTaskModeRemove:
		if err := common.RemoveWalBroadcastTask(ctx, c.client, c.basePath, p.BroadcastID); err != nil {
			return errors.Wrap(err, "failed to remove wal broadcast task")
		}
		fmt.Printf("wal broadcast task removed with broadcast ID %d\n", p.BroadcastID)
	default:
		return fmt.Errorf("invalid mode: %s", p.Mode)
	}
	return nil
}

// repairWalBroadcastTaskByMessageType lists all wal broadcast tasks, picks out the ones
// of the given message type whose ack is incomplete (acked < len(VChannels)), then
// removes them. This mode only supports remove, reset is not allowed.
func (c *ComponentRepair) repairWalBroadcastTaskByMessageType(ctx context.Context, p *WALBroadcastTaskParam) error {
	if p.Mode != repairBroadcastTaskModeRemove {
		return fmt.Errorf("message_type mode only supports remove, got mode: %s", p.Mode)
	}

	mt, ok := messagespb.MessageType_value[p.MessageType]
	if !ok {
		return fmt.Errorf("invalid message type: %s", p.MessageType)
	}

	metas, err := common.ListWalBroadcast(ctx, c.client, c.basePath)
	if err != nil {
		return errors.Wrap(err, "failed to list wal broadcast tasks")
	}

	var targets []*streamingpb.BroadcastTask
	for _, meta := range metas {
		msg := message.NewBroadcastMutableMessageBeforeAppend(meta.Message.Payload, meta.Message.Properties)
		if int32(msg.MessageType()) != mt {
			continue
		}
		bh := msg.BroadcastHeader()
		acked := getAckedCount(meta)
		if acked >= len(bh.VChannels) {
			continue
		}
		targets = append(targets, meta)
	}

	fmt.Printf("Found %d ack-incomplete %s broadcast task(s)\n", len(targets), p.MessageType)
	for _, meta := range targets {
		detail, err := protojson.Marshal(meta)
		if err != nil {
			return errors.Wrap(err, "failed to marshal broadcast task")
		}
		fmt.Printf("%s\n", string(detail))
	}

	if !p.Run {
		return nil
	}

	for _, meta := range targets {
		msg := message.NewBroadcastMutableMessageBeforeAppend(meta.Message.Payload, meta.Message.Properties)
		broadcastID := int64(msg.BroadcastHeader().BroadcastID)
		if err := common.RemoveWalBroadcastTask(ctx, c.client, c.basePath, broadcastID); err != nil {
			return errors.Wrapf(err, "failed to remove wal broadcast task %d", broadcastID)
		}
		fmt.Printf("wal broadcast task removed with broadcast ID %d\n", broadcastID)
	}
	return nil
}

// getAckedCount returns the number of vchannels that have been acked for a broadcast task.
func getAckedCount(meta *streamingpb.BroadcastTask) int {
	acked := 0
	for _, checkpoint := range meta.AckedCheckpoints {
		if checkpoint != nil && checkpoint.TimeTick != 0 {
			acked++
		}
	}
	return acked
}
