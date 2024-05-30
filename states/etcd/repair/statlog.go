package repair

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/spf13/cobra"
)

func RemoveStatLogCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove-statlog",
		Aliases: []string{""},
		Short:   "remove statlog of segment",
		Run: func(cmd *cobra.Command, args []string) {
			fix, err := cmd.Flags().GetBool("fix")
			if err != nil {
				return
			}
			logpath, err := cmd.Flags().GetString("logpath")
			if err != nil {
				return
			}
			p := path.Join(basePath, logpath)
			paths, values, err := cli.LoadWithPrefix(context.Background(), p)
			if err != nil {
				panic(err)
			}
			for i, p := range paths {
				fmt.Printf("%s\n %s\n", p, values[i])
				if fix {
					cli.Remove(context.Background(), p)
				}
			}
		},
	}
	cmd.Flags().String("logpath", "", "log path")
	cmd.Flags().Bool("fix", false, "remove the log path to fix no such key")
	return cmd
}
