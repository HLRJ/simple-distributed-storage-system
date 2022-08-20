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

func TestStat(t *testing.T) {
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
	fileInfo, err := c.Stat(remotePath)
	if err != nil {
		t.Error(err)
	}
	// Calculate the size of local file
	data, err := ioutil.ReadFile(localPath)
	if err != nil {
		t.Error(err)
	}
	localFileSize := uint64(len(data))
	// Compare file name and size
	if fileInfo.Name != remotePath && fileInfo.Size != localFileSize {
		t.Error("Stat failed")
	}
	err = c.CloseClient()
	if err != nil {
		t.Error(err)
	}

}

//func TestMkdir(t *testing.T) {
//	go namenode.NewNameNodeServer().Setup(context.Background())
//
//	time.Sleep(time.Second)
//
//	go datanode.NewDataNodeServer("localhost:9000").Setup(context.Background())
//	go datanode.NewDataNodeServer("localhost:9001").Setup(context.Background())
//	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())
//
//	time.Sleep(5 * time.Second)
//	remotePath := "/doc/test/"
//	c := client.NewClient()
//	err := c.Mkdir(remotePath)
//	if err != nil {
//		t.Error(err)
//	}
//
//	// 目录自身也在自己的files切片里面，不过size为0
//	if len(files.Infos) != 1 {
//		t.Error("Mkdir filed")
//	}
//}

func TestRename(t *testing.T) {
	go namenode.NewNameNodeServer().Setup(context.Background())

	time.Sleep(time.Second)

	go datanode.NewDataNodeServer("localhost:9000").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9001").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())

	time.Sleep(5 * time.Second)

	localPath := "/tmp/README.md"
	remotePath := "/doc/README.md"
	newNamePath := "/doc/new/README.md"
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
	err = c.Rename(remotePath, newNamePath)
	if err != nil {
		t.Error(err)
	}
	err = c.Get(newNamePath, localCopyPath)
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

func TestList(t *testing.T) {
	go namenode.NewNameNodeServer().Setup(context.Background())

	time.Sleep(time.Second)

	go datanode.NewDataNodeServer("localhost:9000").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9001").Setup(context.Background())
	go datanode.NewDataNodeServer("localhost:9002").Setup(context.Background())

	time.Sleep(5 * time.Second)

	localPath := "/tmp/README.md"
	remotePath := "/doc/README.md"
	remoteDirectory := "/doc/"
	c := client.NewClient()
	err := c.Put(localPath, remotePath)
	if err != nil {
		t.Error(err)
	}
	err = c.Mkdir(remoteDirectory)
	if err != nil {
		return
	}
	files, err := c.List(remoteDirectory)
	if err != nil {
		t.Error(err)
	}
	data, err := ioutil.ReadFile(localPath)
	if err != nil {
		t.Error(err)
	}
	localFileSize := uint64(len(data))
	// if return --> remoteDirectory has remoteFile
	for i := 0; i < len(files.Infos); i++ {
		if files.Infos[i].Name == remotePath && files.Infos[i].Size == localFileSize {
			return
		}
	}
	t.Error("List filed")
}
