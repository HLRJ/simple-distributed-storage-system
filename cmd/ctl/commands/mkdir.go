package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"simple-distributed-storage-system/src/client"
)

// 输入 需要创建的远程目录路径 remote_file_path
// 输出 是否成功 result
var mkdirCmd = &cobra.Command{
	Use:   "Mkdir [remote_file_path]",
	Short: "Mkdir object from SDSS cluster",
	Long:  `在分布式文件存储系统中创建给定的目录路径`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "usage: Mkdir [remote_file_path]")
			return
		}

		client := client.NewClient(false)
		defer client.CloseClient()
		err := client.Mkdir(args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
	},
}

func init() {
	rootCmd.AddCommand(mkdirCmd)
}
