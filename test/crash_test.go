package test

import (
	"bytes"
	"context"
	"io/ioutil"
	"simple-distributed-storage-system/src/client"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/datanode"
	"simple-distributed-storage-system/src/namenode"
	"testing"
	"time"
)

func TestCrashOneDataNodeServer(t *testing.T) {
	go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(context.Background())
	go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(context.Background())
	go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(context.Background())
	time.Sleep(5 * time.Second)

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
	err = c.Put(localPath, remotePath)
	if err != nil {
		t.Error(err)
	}

	cancelFunc()
	time.Sleep(5 * time.Second)

	err = c.Get(remotePath, localCopyPath)
	if err != nil {
		t.Error(err)
	}
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
	go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(context.Background())
	go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(context.Background())
	go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(context.Background())
	time.Sleep(5 * time.Second)

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
	err = c.Put(localPath, remotePath)
	if err != nil {
		t.Error(err)
	}

	cancelFunc()
	time.Sleep(time.Second)
	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())
	time.Sleep(5 * time.Second)

	err = c.Get(remotePath, localCopyPath)
	if err != nil {
		t.Error(err)
	}
	dataCopy, err := ioutil.ReadFile(localCopyPath)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(dataCopy, data) {
		panic("inconsistent data")
	}
	c.CloseClient()
}
