package commands

import (
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

//输入 需要获取的远程文件路径 remote _ file _ path
// 输出 远程文件信息，至少应该包含如下：
//文件名
//文件长度
//文件不存在需要返回异常

var statObjectCmd = &cobra.Command{
	Use:   "Stat [remote _ file _ path]",
	Short: "stat object from SDSS cluster",
	Long:  `获取分布式文件存储系统中的文件元数据信息`,
	Run: func(cmd *cobra.Command, args []string) {
		SDSS_Cli := client.NewClient()
		//if err := SDSS_ctl.DownloadFile(args[1], args[2], args[3]); err != nil {
		//	data, _ := json.Marshal(err.Error())
		//	var Options = &pretty.Options{Width: 80, Prefix: "", Indent: "\t", SortKeys: false}
		//	fmt.Printf("%s\n", pretty.Color(pretty.PrettyOptions(data, Options), pretty.TerminalStyle))
		//}
		// 需增加client stat功能
		SDSS_Cli.Get(args[0], args[1])
	},
}

func init() {
	rootCmd.AddCommand(getObjectCmd)
}
