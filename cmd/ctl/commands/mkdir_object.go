package commands

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 需要创建的远程目录路径 remote_file_path
// 输出 是否成功 result
var mkdirObjectCmd = &cobra.Command{
	Use:   "Mkdir [remote_file_path]",
	Short: "Mkdir object from SDSS cluster",
	Long:  `在分布式文件存储系统中创建给定的目录路径`,
	Run: func(cmd *cobra.Command, args []string) {
		client := client.NewClient()
		err := client.Mkdir(args[0])
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
	rootCmd.AddCommand(mkdirObjectCmd)
}
