package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"simple-distributed-storage-system/src/client"
)

// 输入 本地文件路径 local_file_path 远程文件路径 remote_file_path
// 输出 是否成功 result
var putCmd = &cobra.Command{
	Use:   "Put [local_file_path] [remote_file_path]",
	Short: "Put object to remote SDSS cluster",
	Long:  `将本地文件上传分布式文件存储系统`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: Put [local_file_path] [remote_file_path]")
			os.Exit(1)
		}

		client := client.NewClient(false)
		defer client.CloseClient()
		err := client.Put(args[0], args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(putCmd)
}
