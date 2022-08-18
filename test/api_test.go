package test

import (
	"bytes"
	"io/ioutil"
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

	localPath := "/tmp/README.md"
	remotePath := "/doc/README.md"

	c := client.NewClient()
	c.Put(localPath, remotePath)
	c.CloseClient()
}

func TestSimpleGet(t *testing.T) {
	go namenode.Setup()

	time.Sleep(time.Second)

	go datanode.NewDataNodeServer("localhost:9000").Setup()
	go datanode.NewDataNodeServer("localhost:9001").Setup()
	go datanode.NewDataNodeServer("localhost:9002").Setup()

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
