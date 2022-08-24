package commands

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"os"
	"simple-distributed-storage-system/src/client"
	"simple-distributed-storage-system/src/utils"
)

// 输入 需要获取的远程文件路径 remote_file_path
// 输出 远程文件信息
var statCmd = &cobra.Command{
	Use:   "Stat [remote_file_path]",
	Short: "Stat object from SDSS cluster",
	Long:  `获取分布式文件存储系统中的文件元数据信息`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "usage: Stat [remote_file_path]")
			return
		}

		client := client.NewClient(true)
		defer client.CloseClient()
		info, err := client.Stat(args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}

		title := color.New(color.Bold, color.Underline)
		gap := utils.Max(uint64(len(info.Name)), uint64(len("name")))
		title.Printf("%-*v %v\n", gap, "name", "size (bytes)")
		fmt.Printf("%-*v %v\n", gap, info.Name, info.Size)
	},
}

func init() {
	rootCmd.AddCommand(statCmd)
}
