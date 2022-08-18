package commands

import (
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

var getObjectCmd = &cobra.Command{
	Use:   "get_object [meta server addrs] [bucket id] [object name] [local path]",
	Short: "get object from SDSS cluster to local",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		SDSS_Cli := client.NewClient()
		//if err := SDSS_ctl.DownloadFile(args[1], args[2], args[3]); err != nil {
		//	data, _ := json.Marshal(err.Error())
		//	var Options = &pretty.Options{Width: 80, Prefix: "", Indent: "\t", SortKeys: false}
		//	fmt.Printf("%s\n", pretty.Color(pretty.PrettyOptions(data, Options), pretty.TerminalStyle))
		//}
		// 需增加client put功能
		SDSS_Cli.Put(args[1], args[2])
	},
}

func init() {
	rootCmd.AddCommand(getObjectCmd)
}
