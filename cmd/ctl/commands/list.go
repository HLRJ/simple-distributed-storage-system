package commands

import (
	"github.com/spf13/cobra"
)

// 输入 需要 ls 的远程路径 remote_dir_path
// 输出 远程文件信息
var listObjectCmd = &cobra.Command{
	Use:   "List [remote_file_path]",
	Short: "List objects from remote_path of SDSS cluster",
	Long:  `获取分布式文件存储系统中的文件元数据信息`,
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	rootCmd.AddCommand(listObjectCmd)
}
