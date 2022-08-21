package commands

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 原远程路径 rename_src_path 目标远程路径 rename_dest_path
// 输出 是否成功 result
var renameObjectCmd = &cobra.Command{
	Use:   "Rename [rename_src_path] [rename_dest_path]",
	Short: "Rename object from src_path to dest_path",
	Long:  `将分布式文件系统中的原始路径重命名为新的目标路径`,
	Run: func(cmd *cobra.Command, args []string) {
		client := client.NewClient(false)
		err := client.Rename(args[0], args[1])
		if err != nil {
			log.Panic(err)
		}
		client.CloseClient()
	},
}

func init() {
	rootCmd.AddCommand(renameObjectCmd)
}
