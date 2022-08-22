package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"simple-distributed-storage-system/src/client"
)

// 输入 需要读取的远程文件路径 remote_file_path 写入本地文件路径 local_file_path
// 输出 是否成功 result
var getCmd = &cobra.Command{
	Use:   "Get [remote_file_path] [local_file_path]",
	Short: "Get object from SDSS cluster to local",
	Long:  `将分布式文件存储系统中的文件拉取到本地`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: Get [remote_file_path] [local_file_path]")
			return
		}

		client := client.NewClient(true)
		defer client.CloseClient()
		err := client.Get(args[0], args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	},
}

func init() {
	rootCmd.AddCommand(getCmd)
}
