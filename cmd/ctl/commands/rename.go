package commands

import (
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 原远程路径 rename _ src _ path 目标远程路径 rename _ dest_ path
// 输出 是否成功result

var renameObjectCmd = &cobra.Command{
	Use:   "Rename [rename _ src _ path] [rename _ dest_ path]",
	Short: "Rename object from src_path to dest_path",
	Long:  `将分布式文件系统中的原始路径重命名为新的目标路径`,
	Run: func(cmd *cobra.Command, args []string) {
		SDSS_Cli := client.NewClient()
		//if err := SDSS_ctl.DownloadFile(args[1], args[2], args[3]); err != nil {
		//	data, _ := json.Marshal(err.Error())
		//	var Options = &pretty.Options{Width: 80, Prefix: "", Indent: "\t", SortKeys: false}
		//	fmt.Printf("%s\n", pretty.Color(pretty.PrettyOptions(data, Options), pretty.TerminalStyle))
		//}
		// 需增加client rename功能
		SDSS_Cli.Get(args[0], args[1])
	},
}

func init() {
	rootCmd.AddCommand(getObjectCmd)
}
