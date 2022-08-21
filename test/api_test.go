package test

import (
	"bytes"
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"simple-distributed-storage-system/src/client"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/datanode"
	"simple-distributed-storage-system/src/namenode"
	"testing"
	"time"
)

func TestAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "API TESTS")
}

var _ = Describe("API TESTS", func() {
	BeforeEach(func() {
		err := os.RemoveAll(consts.RaftPersistenceDataDir)
		if err != nil {
			log.Warn(err)
		}
	})

	AfterEach(func() {
		// wait for resources to be released
		time.Sleep(5 * time.Second)
	})

	It("Put", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		time.Sleep(5 * time.Second)

		localPath := "../README.md"
		remotePath := "/README.md"

		c := client.NewClient()
		err := c.Put(localPath, remotePath)
		Expect(err).To(BeNil())
		c.CloseClient()

		cancelFunc()
	})

	It("Get", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		time.Sleep(5 * time.Second)

		localPath := "../README.md"
		remotePath := "/README.md"
		localCopyPath := "/tmp/README.md"

		c := client.NewClient()
		data, err := ioutil.ReadFile(localPath)
		Expect(err).To(BeNil())
		err = c.Put(localPath, remotePath)
		Expect(err).To(BeNil())
		err = c.Get(remotePath, localCopyPath)
		Expect(err).To(BeNil())
		dataCopy, err := ioutil.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())
		c.CloseClient()

		cancelFunc()
	})

	It("Remove", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		time.Sleep(5 * time.Second)

		localPath := "../README.md"
		remotePath := "/README.md"
		localCopyPath := "/tmp/README.md"

		c := client.NewClient()
		err := c.Put(localPath, remotePath)
		Expect(err).To(BeNil())
		err = c.Remove(remotePath)
		Expect(err).To(BeNil())
		err = c.Get(remotePath, localCopyPath)
		// should be error
		Expect(err).ToNot(BeNil())
		c.CloseClient()

		cancelFunc()
	})

	It("Stat", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		time.Sleep(5 * time.Second)

		localPath := "../README.md"
		remotePath := "/README.md"
		c := client.NewClient()
		err := c.Put(localPath, remotePath)
		Expect(err).To(BeNil())
		fileInfo, err := c.Stat(remotePath)
		Expect(err).To(BeNil())
		// calculate the size of local file
		data, err := ioutil.ReadFile(localPath)
		Expect(err).To(BeNil())
		localFileSize := uint64(len(data))
		// compare file name and size
		Expect(fileInfo.Name).To(Equal(remotePath))
		Expect(fileInfo.Size).To(Equal(localFileSize))
		c.CloseClient()

		cancelFunc()
	})

	It("Mkdir", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)

		time.Sleep(5 * time.Second)
		remotePath := "/doc/"
		c := client.NewClient()
		err := c.Mkdir(remotePath)
		Expect(err).To(BeNil())
		files, err := c.List(remotePath)
		Expect(err).To(BeNil())
		// the length of new directory is 1，and size is 0
		Expect(len(files)).To(Equal(1))
		Expect(files[0].Size).To(Equal(uint64(0)))
		Expect(files[0].Name).To(Equal(remotePath))

		cancelFunc()
	})

	It("Rename", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		time.Sleep(5 * time.Second)

		localPath := "../README.md"
		remotePath := "/README.md"
		newNamePath := "/foo.md"
		localCopyPath := "/tmp/foo.md"

		c := client.NewClient()
		data, err := ioutil.ReadFile(localPath)
		Expect(err).To(BeNil())
		err = c.Put(localPath, remotePath)
		Expect(err).To(BeNil())
		err = c.Rename(remotePath, newNamePath)
		Expect(err).To(BeNil())
		err = c.Get(newNamePath, localCopyPath)
		Expect(err).To(BeNil())
		dataCopy, err := ioutil.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())
		c.CloseClient()

		cancelFunc()
	})

	It("List", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		time.Sleep(5 * time.Second)

		localPath := "/tmp/README.md"
		remotePath := "/doc/README.md"
		remoteDirectory := "/doc/"
		c := client.NewClient()
		err := c.Mkdir(remoteDirectory)
		Expect(err).To(BeNil())
		err = c.Put(localPath, remotePath)
		Expect(err).To(BeNil())
		files, err := c.List(remoteDirectory)
		Expect(err).To(BeNil())
		data, err := ioutil.ReadFile(localPath)
		Expect(err).To(BeNil())
		localFileSize := uint64(len(data))

		Expect(len(files)).To(Equal(2))
		// TODO: order
		Expect(files[0].Size).To(Equal(uint64(0)))
		Expect(files[0].Name).To(Equal(remoteDirectory))
		Expect(files[1].Size).To(Equal(localFileSize))
		Expect(files[1].Name).To(Equal(remotePath))

		cancelFunc()
	})
})
