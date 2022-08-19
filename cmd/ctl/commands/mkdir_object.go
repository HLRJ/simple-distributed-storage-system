package commands

import (
	"github.com/spf13/cobra"
)

// 输入 需要创建的远程目录路径 remote_file_path
// 输出 是否成功 result
var mkdirObjectCmd = &cobra.Command{
	Use:   "Mkdir [remote_file_path]",
	Short: "Mkdir object from SDSS cluster",
	Long:  `在分布式文件存储系统中创建给定的目录路径`,
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	rootCmd.AddCommand(mkdirObjectCmd)
}
