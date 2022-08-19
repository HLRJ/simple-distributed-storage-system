package test

import (
	"context"
	"simple-distributed-storage-system/src/client"
	"simple-distributed-storage-system/src/datanode"
	"simple-distributed-storage-system/src/namenode"
	"testing"
	"time"
)

func TestCrashOneDataNodeServer(t *testing.T) {
	go namenode.NewNameNodeServer().Setup(context.Background())

	time.Sleep(time.Second)

	ctx, cancelFunc := context.WithCancel(context.Background())
	go datanode.NewDataNodeServer("localhost:9000").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9001").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9003").Setup(ctx)

	time.Sleep(5 * time.Second)

	localPath := "/tmp/README.md"
	remotePath := "/doc/README.md"

	c := client.NewClient()
	c.Put(localPath, remotePath)
	c.CloseClient()

	cancelFunc()

	select {}
}

func TestCrashNameNodeServer(t *testing.T) { // TODO
	ctx, cancelFunc := context.WithCancel(context.Background())
	go namenode.NewNameNodeServer().Setup(ctx)

	time.Sleep(time.Second)

	go datanode.NewDataNodeServer("localhost:9000").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9001").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())

	time.Sleep(5 * time.Second)

	localPath := "/tmp/README.md"
	remotePath := "/doc/README.md"

	c := client.NewClient()
	c.Put(localPath, remotePath)
	c.CloseClient()

	cancelFunc()

	select {}
}

func TestCrashOneDataNodeServerAndReconnect(t *testing.T) {
	go namenode.NewNameNodeServer().Setup(context.Background())

	time.Sleep(time.Second)

	ctx, cancelFunc := context.WithCancel(context.Background())
	go datanode.NewDataNodeServer("localhost:9000").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9001").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)

	time.Sleep(5 * time.Second)

	localPath := "/tmp/README.md"
	remotePath := "/doc/README.md"

	c := client.NewClient()
	c.Put(localPath, remotePath)
	c.CloseClient()

	cancelFunc()

	time.Sleep(time.Second)

	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())

	select {}
}
