package commands

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 需要删除的远程文件路径 remote_file_path
// 输出 删除结果 result
var delObjectCmd = &cobra.Command{
	Use:   "Delete [remote_file_path]",
	Short: "Del object from SDSS cluster",
	Long:  `删除分布式文件存储系统中的文件`,
	Run: func(cmd *cobra.Command, args []string) {
		client := client.NewClient(false)
		err := client.Remove(args[0])
		if err != nil {
			log.Panic(err)
		}
		client.CloseClient()
	},
}

func init() {
	rootCmd.AddCommand(delObjectCmd)
}
