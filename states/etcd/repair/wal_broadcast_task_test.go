package repair

import (
	"context"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type walBroadcastKV struct {
	kv.MetaKV
	data map[string]string
}

func (w *walBroadcastKV) Load(ctx context.Context, key string, opts ...kv.LoadOption) (string, error) {
	value, ok := w.data[key]
	if !ok {
		return "", kv.ErrKeyNotFound
	}
	return value, nil
}

func (w *walBroadcastKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
	keys := make([]string, 0)
	for key := range w.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, w.data[key])
	}
	return keys, values, nil
}

func (w *walBroadcastKV) Save(ctx context.Context, key, value string) error {
	w.data[key] = value
	return nil
}

func (w *walBroadcastKV) Remove(ctx context.Context, key string) error {
	delete(w.data, key)
	return nil
}

func (w *walBroadcastKV) RemoveWithPrefix(ctx context.Context, key string) error {
	for k := range w.data {
		if strings.HasPrefix(k, key) {
			delete(w.data, k)
		}
	}
	return nil
}

func newTestBroadcastTask(msgType message.MessageType, broadcastID uint64, vchannels []string, acked []uint64, state streamingpb.BroadcastTaskState) *streamingpb.BroadcastTask {
	bh, err := message.EncodeProto(&messagespb.BroadcastHeader{
		BroadcastId: broadcastID,
		Vchannels:   vchannels,
	})
	if err != nil {
		panic(err)
	}
	checkpoints := make([]*streamingpb.AckedCheckpoint, len(vchannels))
	for i, ts := range acked {
		checkpoints[i] = &streamingpb.AckedCheckpoint{TimeTick: ts}
	}
	return &streamingpb.BroadcastTask{
		Message: &messagespb.Message{
			Payload: []byte("test-payload"),
			Properties: map[string]string{
				"_t":  strconv.FormatInt(int64(msgType), 10),
				"_bh": bh,
			},
		},
		State:            state,
		AckedCheckpoints: checkpoints,
	}
}

func TestRepairWalBroadcastTaskByMessageType(t *testing.T) {
	basePath := "by-dev"
	cli := &walBroadcastKV{data: make(map[string]string)}
	comp := NewComponent(cli, nil, basePath)

	writeTask := func(id uint64, meta *streamingpb.BroadcastTask) {
		bs, err := proto.Marshal(meta)
		require.NoError(t, err)
		cli.data[path.Join(basePath, "streamingcoord-meta/broadcast-task", strconv.FormatUint(id, 10))] = string(bs)
	}

	// two ack-incomplete DropCollection tasks, one ack-complete DropCollection task,
	// one ack-incomplete task of a different type.
	writeTask(1, newTestBroadcastTask(message.MessageTypeDropCollection, 1, []string{"v1", "v2"}, []uint64{100}, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING))
	writeTask(2, newTestBroadcastTask(message.MessageTypeDropCollection, 2, []string{"v1"}, nil, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING))
	writeTask(3, newTestBroadcastTask(message.MessageTypeDropCollection, 3, []string{"v1", "v2"}, []uint64{100, 200}, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE))
	writeTask(4, newTestBroadcastTask(message.MessageTypeCreateCollection, 4, []string{"v1", "v2"}, []uint64{100}, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING))

	ctx := context.Background()

	// dry-run: should report 2 targets, not modify anything.
	err := comp.WALBroadcastTaskCommand(ctx, &WALBroadcastTaskParam{
		MessageType: "DropCollection",
		Mode:        repairBroadcastTaskModeRemove,
	})
	require.NoError(t, err)
	require.Len(t, cli.data, 4)

	// run: remove the 2 ack-incomplete DropCollection tasks only.
	err = comp.WALBroadcastTaskCommand(ctx, &WALBroadcastTaskParam{
		MessageType:    "DropCollection",
		Mode:           repairBroadcastTaskModeRemove,
		ExecutionParam: framework.ExecutionParam{Run: true},
	})
	require.NoError(t, err)
	require.Len(t, cli.data, 2)
	_, ok := cli.data[path.Join(basePath, "streamingcoord-meta/broadcast-task", "3")]
	require.True(t, ok, "ack-complete task should be kept")
	_, ok = cli.data[path.Join(basePath, "streamingcoord-meta/broadcast-task", "4")]
	require.True(t, ok, "different-type task should be kept")
	_, ok = cli.data[path.Join(basePath, "streamingcoord-meta/broadcast-task", "1")]
	require.False(t, ok, "ack-incomplete DropCollection task should be removed")
	_, ok = cli.data[path.Join(basePath, "streamingcoord-meta/broadcast-task", "2")]
	require.False(t, ok, "ack-incomplete DropCollection task should be removed")
}

func TestRepairWalBroadcastTaskByMessageTypeRejectsReset(t *testing.T) {
	cli := &walBroadcastKV{data: make(map[string]string)}
	comp := NewComponent(cli, nil, "by-dev")

	err := comp.WALBroadcastTaskCommand(context.Background(), &WALBroadcastTaskParam{
		MessageType: "DropCollection",
		Mode:        repairBroadcastTaskModeReset,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "only supports remove")
}

func TestRepairWalBroadcastTaskByMessageTypeInvalidType(t *testing.T) {
	cli := &walBroadcastKV{data: make(map[string]string)}
	comp := NewComponent(cli, nil, "by-dev")

	err := comp.WALBroadcastTaskCommand(context.Background(), &WALBroadcastTaskParam{
		MessageType: "NotAType",
		Mode:        repairBroadcastTaskModeRemove,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid message type")
}

func TestRepairWalBroadcastTaskByMessageTypeEmptyStore(t *testing.T) {
	cli := &walBroadcastKV{data: make(map[string]string)}
	comp := NewComponent(cli, nil, "by-dev")

	err := comp.WALBroadcastTaskCommand(context.Background(), &WALBroadcastTaskParam{
		MessageType: "DropCollection",
		Mode:        repairBroadcastTaskModeRemove,
	})
	require.NoError(t, err)
}
