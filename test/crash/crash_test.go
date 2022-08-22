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

	localPath := "../../LICENSE"
	remotePath := "/LICENSE"
	localCopyPath := "/tmp/LICENSE"

	It("Crash one datanode server", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		ctxTarget, cancelFuncTarget := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9003").Setup(ctxTarget)

		// wait for setup
		time.Sleep(5 * time.Second)

		c := client.NewClient(false)
		defer c.CloseClient()

		data, err := os.ReadFile(localPath)
		Expect(err).To(BeNil())

		err = c.Put(localPath, remotePath)
		Expect(err).To(BeNil())

		cancelFuncTarget()
		time.Sleep(5 * time.Second) // for data migration

		err = c.Get(remotePath, localCopyPath)
		Expect(err).To(BeNil())

		dataCopy, err := os.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())
	})

	It("Crash one datanode server and reconnect", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		ctxTarget, cancelFuncTarget := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctxTarget)

		// wait for setup
		time.Sleep(5 * time.Second)

		c := client.NewClient(false)
		defer c.CloseClient()

		data, err := os.ReadFile(localPath)
		Expect(err).To(BeNil())

		err = c.Put(localPath, remotePath)
		Expect(err).To(BeNil())

		cancelFuncTarget()
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		time.Sleep(5 * time.Second) // for registration

		err = c.Get(remotePath, localCopyPath)
		Expect(err).To(BeNil())

		dataCopy, err := os.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())
	})

	It("Crash one namenode server", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		ctxTarget, cancelFuncTarget := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctxTarget)
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

		time.Sleep(5 * time.Second) // for sync read
		cancelFuncTarget()

		err = c.Get(remotePath, localCopyPath)
		Expect(err).To(BeNil())

		dataCopy, err := os.ReadFile(localCopyPath)
		Expect(err).To(BeNil())
		Expect(bytes.Equal(dataCopy, data)).To(BeTrue())
	})
})
