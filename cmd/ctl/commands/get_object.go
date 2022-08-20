package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 需要读取的远程文件路径 remote_file_path 写入本地文件路径 local_file_path
// 输出 是否成功 result
var getObjectCmd = &cobra.Command{
	Use:   "Get [remote_file_path] [local_file_path]",
	Short: "Get object from SDSS cluster to local",
	Long:  `将分布式文件存储系统中的文件拉取到本地`,
	Run: func(cmd *cobra.Command, args []string) {
		client := client.NewReadonlyClient()
		err := client.Get(args[0], args[1])
		if err != nil {
			fmt.Println("get file error")
		}
		client.CloseClient()
		fmt.Println("get successfully")
	},
}

func init() {
	rootCmd.AddCommand(getObjectCmd)
}
