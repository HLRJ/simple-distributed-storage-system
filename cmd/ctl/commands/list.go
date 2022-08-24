package commands

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"os"
	"simple-distributed-storage-system/src/client"
	"simple-distributed-storage-system/src/utils"
	"sort"
	"strings"
)

// 输入 需要 ls 的远程路径 remote_dir_path
// 输出 远程文件信息
var listCmd = &cobra.Command{
	Use:   "List [remote_file_path]",
	Short: "List objects from remote_path of SDSS cluster",
	Long:  `获取分布式文件存储系统中的文件元数据信息`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "usage: List [remote_file_path]")
			os.Exit(1)
		}

		client := client.NewClient(true)
		defer client.CloseClient()
		infos, err := client.List(args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		sort.Slice(infos, func(i, j int) bool {
			return strings.Compare(infos[i].Name, infos[j].Name) < 0
		})
		title := color.New(color.Bold, color.Underline)
		gap := utils.Max(uint64(len(infos[len(infos)-1].Name)), uint64(len("name")))
		title.Printf("%-*v %v\n", gap, "name", "size (bytes)")
		for _, info := range infos {
			fmt.Printf("%-*v %v\n", gap, info.Name, info.Size)
		}
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}
