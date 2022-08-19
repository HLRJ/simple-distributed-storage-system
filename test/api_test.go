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

func TestSimplePut(t *testing.T) {
	go namenode.NewNameNodeServer().Setup(context.Background())

	time.Sleep(time.Second)

	go datanode.NewDataNodeServer("localhost:9000").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9001").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())

	time.Sleep(5 * time.Second)

	localPath := "/tmp/README.md"
	remotePath := "/doc/README.md"

	c := client.NewClient()
	err := c.Put(localPath, remotePath)
	if err != nil {
		t.Error(err)
	}
	err = c.CloseClient()
	if err != nil {
		t.Error(err)
	}
}

func TestSimpleGet(t *testing.T) {
	go namenode.NewNameNodeServer().Setup(context.Background())

	time.Sleep(time.Second)

	go datanode.NewDataNodeServer("localhost:9000").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9001").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())

	time.Sleep(5 * time.Second)

	localPath := "/tmp/README.md"
	remotePath := "/doc/README.md"
	localCopyPath := "/tmp/foo.md"

	c := client.NewClient()
	data, err := ioutil.ReadFile(localPath)
	if err != nil {
		t.Error(err)
	}
	err = c.Put(localPath, remotePath)
	if err != nil {
		t.Error(err)
	}
	err = c.Get(remotePath, localCopyPath)
	if err != nil {
		t.Error(err)
	}
	dataCopy, err := ioutil.ReadFile(localCopyPath)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(dataCopy, data) {
		t.Error("inconsistent data")
	}
	err = c.CloseClient()
	if err != nil {
		t.Error(err)
	}
}

func TestSimpleRemove(t *testing.T) {
	go namenode.NewNameNodeServer().Setup(context.Background())

	time.Sleep(time.Second)

	go datanode.NewDataNodeServer("localhost:9000").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9001").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())

	time.Sleep(5 * time.Second)

	localPath := "/tmp/README.md"
	remotePath := "/doc/README.md"
	localCopyPath := "/tmp/foo.md"

	c := client.NewClient()
	err := c.Put(localPath, remotePath)
	if err != nil {
		t.Error(err)
	}
	err = c.Remove(remotePath)
	if err != nil {
		t.Error(err)
	}
	err = c.Get(remotePath, localCopyPath)
	if err == nil {
		t.Error("remove failed")
	}
	err = c.CloseClient()
	if err != nil {
		t.Error(err)
	}
}
