package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"simple-distributed-storage-system/src/client"
)

// 输入 需要删除的远程文件路径 remote_file_path
// 输出 删除结果 result
var deleteCmd = &cobra.Command{
	Use:   "Delete [remote_file_path]",
	Short: "Delete object from SDSS cluster",
	Long:  `删除分布式文件存储系统中的文件`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "usage: Delete [remote_file_path]")
			os.Exit(1)
		}

		client := client.NewClient(false)
		defer client.CloseClient()
		err := client.Remove(args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}
