package test

import (
	"bytes"
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
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

	localPath := "../../LICENSE"
	remotePath := "/LICENSE"
	localCopyPath := "/tmp/LICENSE"
	remoteDir := "/doc/"
	remoteNewPath := "/LICENSE_NEW"
	remotePathWithDir := "/doc/LICENSE"

	It("Put", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)

		// wait for setup
		time.Sleep(5 * time.Second)

		c := client.NewClient(false)
		defer c.CloseClient()

		err := c.Put(localPath, remotePath)
		Expect(err).To(BeNil())
	})

	It("Get", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)

		// wait for setup
		time.Sleep(5 * time.Second)

		c := client.NewClient(false)
		defer c.CloseClient()

		data, err := os.ReadFile(localPath)
		Expect(err).To(BeNil())

		err = c.Put(localPath, remotePath)
		Expect(err).To(BeNil())

		err = c.Get(remotePath, localCopyPath)
		Expect(err).To(BeNil())

		dataCopy, err := os.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())
	})

	It("Remove", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)

		// wait for setup
		time.Sleep(5 * time.Second)

		c := client.NewClient(false)
		defer c.CloseClient()

		err := c.Put(localPath, remotePath)
		Expect(err).To(BeNil())

		err = c.Remove(remotePath)
		Expect(err).To(BeNil())

		err = c.Get(remotePath, localCopyPath)
		// should be error
		Expect(err).ToNot(BeNil())
	})

	It("Stat", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)

		// wait for setup
		time.Sleep(5 * time.Second)

		c := client.NewClient(false)
		defer c.CloseClient()

		err := c.Put(localPath, remotePath)
		Expect(err).To(BeNil())

		fileInfo, err := c.Stat(remotePath)
		Expect(err).To(BeNil())

		// calculate the size of local file
		data, err := os.ReadFile(localPath)
		Expect(err).To(BeNil())

		// compare file name and size
		Expect(fileInfo.Name).To(Equal(remotePath))
		Expect(fileInfo.Size).To(Equal(uint64(len(data))))
	})

	It("Mkdir", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)

		// wait for setup
		time.Sleep(5 * time.Second)

		c := client.NewClient(false)
		defer c.CloseClient()

		err := c.Mkdir(remoteDir)
		Expect(err).To(BeNil())

		files, err := c.List(remoteDir)
		Expect(err).To(BeNil())

		// the length of new directory is 1???and size is 0
		Expect(len(files)).To(Equal(1))
		Expect(files[0].Size).To(Equal(uint64(0)))
		Expect(files[0].Name).To(Equal(remoteDir))
	})

	It("Rename", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)

		// wait for setup
		time.Sleep(5 * time.Second)

		c := client.NewClient(false)
		defer c.CloseClient()

		data, err := os.ReadFile(localPath)
		Expect(err).To(BeNil())

		err = c.Put(localPath, remotePath)
		Expect(err).To(BeNil())

		err = c.Rename(remotePath, remoteNewPath)
		Expect(err).To(BeNil())

		err = c.Get(remoteNewPath, localCopyPath)
		Expect(err).To(BeNil())

		dataCopy, err := os.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())
	})

	It("List", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)

		// wait for setup
		time.Sleep(5 * time.Second)

		c := client.NewClient(false)
		defer c.CloseClient()

		err := c.Mkdir(remoteDir)
		Expect(err).To(BeNil())

		err = c.Put(localPath, remotePathWithDir)
		Expect(err).To(BeNil())

		fileInfos, err := c.List(remoteDir)
		Expect(err).To(BeNil())

		data, err := os.ReadFile(localPath)
		Expect(err).To(BeNil())

		Expect(len(fileInfos)).To(Equal(2))
		if fileInfos[0].Size == uint64(0) && fileInfos[0].Name == remoteDir &&
			fileInfos[1].Size == uint64(len(data)) && fileInfos[1].Name == remotePathWithDir {
			return
		}
		if fileInfos[1].Size == uint64(0) && fileInfos[1].Name == remoteDir &&
			fileInfos[0].Size == uint64(len(data)) && fileInfos[0].Name == remotePathWithDir {
			return
		}
		Fail("the result of LIST is mismatching")
	})
})
