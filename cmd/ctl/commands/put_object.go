package commands

import (
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

var putObjectCmd = &cobra.Command{
	Use:   "put_object [meta server addrs] [local file path] [bucket id]",
	Short: "put object to remote SDSS cluster",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		SDSS_Cli := client.NewClient() //需增加namenode的地址
		//objectId, err := SDSS_Cli.Put(args[1], args[2]) //需增加错误处理
		//if err != nil {
		//	fmt.Printf("%s\n", err.Error())
		//}
		//fmt.Printf("put object return %s\n", objectId)
		SDSS_Cli.Put(args[1], args[2])
	},
}

func init() {
	rootCmd.AddCommand(putObjectCmd)
}
