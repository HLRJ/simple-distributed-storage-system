package test

import (
	"bytes"
	"context"
	"io/ioutil"
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
	localCopyPath := "/tmp/foo.md"

	c := client.NewClient()
	data, err := ioutil.ReadFile(localPath)
	if err != nil {
		panic(err)
	}
	c.Put(localPath, remotePath)

	cancelFunc()
	time.Sleep(5 * time.Second)

	c.Get(remotePath, localCopyPath)
	dataCopy, err := ioutil.ReadFile(localCopyPath)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(dataCopy, data) {
		panic("inconsistent data")
	}
	c.CloseClient()
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
	localCopyPath := "/tmp/foo.md"

	c := client.NewClient()
	data, err := ioutil.ReadFile(localPath)
	if err != nil {
		panic(err)
	}
	c.Put(localPath, remotePath)

	cancelFunc()
	time.Sleep(time.Second)
	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())
	time.Sleep(5 * time.Second)

	c.Get(remotePath, localCopyPath)
	dataCopy, err := ioutil.ReadFile(localCopyPath)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(dataCopy, data) {
		panic("inconsistent data")
	}
	c.CloseClient()
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
