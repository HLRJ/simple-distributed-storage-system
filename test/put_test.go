package test

import (
	"simple-distributed-storage-system/src/client"
	"simple-distributed-storage-system/src/datanode"
	"simple-distributed-storage-system/src/namenode"
	"testing"
	"time"
)

func TestSimplePut(t *testing.T) {
	go namenode.Setup()

	time.Sleep(time.Second)

	go datanode.NewDataNodeServer("localhost:9000").Setup()
	go datanode.NewDataNodeServer("localhost:9001").Setup()
	go datanode.NewDataNodeServer("localhost:9002").Setup()

	time.Sleep(5 * time.Second)

	c := client.NewClient()
	c.Put("/tmp/README.md", "/doc/README.md")
	c.CloseClient()
}
