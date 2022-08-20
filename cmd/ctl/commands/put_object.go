package commands

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 本地文件路径 local_file_path 远程文件路径 remote_file_path
// 输出 是否成功 result
var putObjectCmd = &cobra.Command{
	Use:   "Put [local_file_path] [remote_file_path]",
	Short: "Put object to remote SDSS cluster",
	Long:  `将本地文件上传分布式文件存储系统`,
	Run: func(cmd *cobra.Command, args []string) {
		client := client.NewClient()
		err := client.Put(args[0], args[1])
		if err != nil {
			fmt.Println("put file error")
		}
		err = client.CloseClient()
		if err != nil {
			log.Panic(err)
		}
		fmt.Println("put successfully")
	},
}

func init() {
	rootCmd.AddCommand(putObjectCmd)
}
