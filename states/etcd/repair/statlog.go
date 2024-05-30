package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/spf13/cobra"
)

func RemoveStatLogCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove-statlog",
		Aliases: []string{""},
		Short:   "remove statlog of segment",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				return
			}
			segmentID, err := cmd.Flags().GetInt64("segment")
			if err != nil {
				return
			}
			partitionID, err := cmd.Flags().GetInt64("partition")
			if err != nil {
				return
			}
			segments, err := common.ListSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
				return (info.CollectionID == collectionID) &&
					(info.ID == segmentID)
			})
			if err != nil {
				return
			}
			for _, s := range segments {
				if err := json.NewEncoder(os.Stdout).Encode(s); err != nil {
					panic(err)
				}
				fmt.Println()
			}
			fix, err := cmd.Flags().GetBool("fix")
			if err != nil {
				return
			}
			for _, s := range segments {
				for _, f := range s.GetStatslogs() {
					for _, l := range f.GetBinlogs() {
						i := strings.Split(l.LogPath, "/")
						logID, err := strconv.ParseInt(i[len(i)-1], 10, 64)
						if err != nil {
							panic(err)
						}
						fmt.Printf("remove log path: %s\n", l.LogPath)
						fmt.Printf("%d, %d, %d, %d, %d\n", collectionID, partitionID, segmentID, f.FieldID, logID)
						if fix {
							common.RemoveSegmentStatLogPath(context.Background(), cli, basePath, collectionID, partitionID, segmentID, f.FieldID, logID)
						}
					}
				}
			}
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id")
	cmd.Flags().Int64("partition", 0, "partition id")
	cmd.Flags().Int64("segment", 0, "segment id")
	cmd.Flags().Bool("fix", false, "remove the log path to fix no such key")
	return cmd
}
