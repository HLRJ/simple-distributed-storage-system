package commands

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"simple-distributed-storage-system/src/client"
)

// 输入 需要获取的远程文件路径 remote_file_path
// 输出 远程文件信息
var statObjectCmd = &cobra.Command{
	Use:   "Stat [remote_file_path]",
	Short: "Stat object from SDSS cluster",
	Long:  `获取分布式文件存储系统中的文件元数据信息`,
	Run: func(cmd *cobra.Command, args []string) {
		client := client.NewClient()
		fileInfo, err := client.Stat(args[0])
		if err != nil {
			fmt.Println("stat error")
			log.Panic(err)
		}
		err = client.CloseClient()
		if err != nil {
			log.Panic(err)
		}
		//输出 文件信息
		fmt.Println(fileInfo)
	},
}

func init() {
	rootCmd.AddCommand(statObjectCmd)
}
