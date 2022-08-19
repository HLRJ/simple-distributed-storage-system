package commands

import (
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 本地文件路径 local _ file _ path 远程文件路径 remote _ file _ path
// 输出 是否成功result
var putObjectCmd = &cobra.Command{
	Use:   "Put [local _ file _ path] [remote _ file _ path]",
	Short: "put object to remote SDSS cluster",
	Long:  `将本地文件上传分布式文件存储系统`,
	Run: func(cmd *cobra.Command, args []string) {
		SDSS_Cli := client.NewClient()
		//objectId, err := SDSS_Cli.Put(args[1], args[2]) //需增加错误处理
		//if err != nil {
		//	fmt.Printf("%s\n", err.Error())
		//}
		//fmt.Printf("put object return %s\n", objectId)
		SDSS_Cli.Put(args[0], args[1])
	},
}

func init() {
	rootCmd.AddCommand(putObjectCmd)
}
