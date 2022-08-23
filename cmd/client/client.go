package main

import (
	log "github.com/sirupsen/logrus"
	"simple-distributed-storage-system/src/client"
)

func main() {
	c := client.NewClient(false)
	_, err := c.List("/")
	if err != nil {
		log.Panic(err)
	}
}

