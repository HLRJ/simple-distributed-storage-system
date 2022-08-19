package commands

import (
	log "github.com/sirupsen/logrus"
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
		client := client.NewClient()
		err := client.Get(args[0], args[1])
		if err != nil {
			log.Panic(err)
		}
		err = client.CloseClient()
		if err != nil {
			log.Panic(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(getObjectCmd)
}
