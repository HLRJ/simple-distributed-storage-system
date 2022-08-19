package commands

import (
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 需要读取的远程文件路径 remote _ file _ path  写入本地文件路径 local _ file _ path
// 输出 是否成功result
var getObjectCmd = &cobra.Command{
	Use:   "Get [remote _ file _ path] [local _ file _ path]",
	Short: "get object from SDSS cluster to local",
	Long:  `将分布式文件存储系统中的文件拉取到本地`,
	Run: func(cmd *cobra.Command, args []string) {
		SDSS_Cli := client.NewClient()
		//if err := SDSS_ctl.DownloadFile(args[1], args[2], args[3]); err != nil {
		//	data, _ := json.Marshal(err.Error())
		//	var Options = &pretty.Options{Width: 80, Prefix: "", Indent: "\t", SortKeys: false}
		//	fmt.Printf("%s\n", pretty.Color(pretty.PrettyOptions(data, Options), pretty.TerminalStyle))
		//}
		// 需增加client put功能
		SDSS_Cli.Get(args[0], args[1])
	},
}

func init() {
	rootCmd.AddCommand(getObjectCmd)
}
