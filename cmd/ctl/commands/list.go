package commands

import (
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 需要 ls 的远程路径 remote _ dir _ path
// 输出 远程文件信息，至少应该包含如下：
//文件名
//文件长度

var listObjectCmd = &cobra.Command{
	Use:   "List [remote _ file _ path]",
	Short: "List objects from remote_path of SDSS cluster",
	Long:  `获取分布式文件存储系统中的文件元数据信息`,
	Run: func(cmd *cobra.Command, args []string) {
		SDSS_Cli := client.NewClient()
		//if err := SDSS_ctl.DownloadFile(args[1], args[2], args[3]); err != nil {
		//	data, _ := json.Marshal(err.Error())
		//	var Options = &pretty.Options{Width: 80, Prefix: "", Indent: "\t", SortKeys: false}
		//	fmt.Printf("%s\n", pretty.Color(pretty.PrettyOptions(data, Options), pretty.TerminalStyle))
		//}
		// 需增加client list功能
		SDSS_Cli.Get(args[0], args[1])
	},
}

func init() {
	rootCmd.AddCommand(getObjectCmd)
}
