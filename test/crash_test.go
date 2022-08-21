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

func TestCrash(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "CRASH TESTS")
}

var _ = Describe("CRASH TESTS", func() {
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

	It("Crash one datanode server", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		ctxTarget, cancelFuncTarget := context.WithCancel(context.Background())
		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9003").Setup(ctxTarget)
		time.Sleep(5 * time.Second)

		localPath := "../README.md"
		remotePath := "/README.md"
		localCopyPath := "/tmp/README.md"

		c := client.NewClient()
		data, err := ioutil.ReadFile(localPath)
		Expect(err).To(BeNil())
		err = c.Put(localPath, remotePath)
		Expect(err).To(BeNil())

		cancelFuncTarget()
		time.Sleep(5 * time.Second) // for data migration

		err = c.Get(remotePath, localCopyPath)
		Expect(err).To(BeNil())
		dataCopy, err := ioutil.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())

		c.CloseClient()
		cancelFunc()
	})

	It("Crash one datanode server and reconnect", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		ctxTarget, cancelFuncTarget := context.WithCancel(context.Background())
		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctxTarget)
		time.Sleep(5 * time.Second)

		localPath := "../README.md"
		remotePath := "/README.md"
		localCopyPath := "/tmp/README.md"

		c := client.NewClient()
		data, err := ioutil.ReadFile(localPath)
		Expect(err).To(BeNil())
		err = c.Put(localPath, remotePath)
		Expect(err).To(BeNil())

		cancelFuncTarget()
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		time.Sleep(5 * time.Second) // for registration

		err = c.Get(remotePath, localCopyPath)
		Expect(err).To(BeNil())
		dataCopy, err := ioutil.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())

		c.CloseClient()
		cancelFunc()
	})

	FIt("Crash one namenode server", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		ctxTarget, cancelFuncTarget := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctxTarget)
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
		c.CloseClient()

		time.Sleep(5 * time.Second) // for sync read
		cancelFuncTarget()
		time.Sleep(5 * time.Second) // for new leader

		c = client.NewClient() // new client
		err = c.Get(remotePath, localCopyPath)
		Expect(err).To(BeNil())
		dataCopy, err := ioutil.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())

		c.CloseClient()
		cancelFunc()
	})
})
