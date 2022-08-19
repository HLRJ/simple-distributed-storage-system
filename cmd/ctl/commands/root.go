package commands

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "SDSS",
	Short: "SDSS is a simple distributed storage system",
	Long: `A simple distributed storage system built in Go.
Complete documentation is available at https://github.com/HLRJ/simple-distributed-storage-system`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("./SDSS-ctl -h for help")
			return
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Panic(err)
	}
}
