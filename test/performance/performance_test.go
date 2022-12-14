package test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"simple-distributed-storage-system/src/client"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/datanode"
	"simple-distributed-storage-system/src/namenode"
	"testing"
	"time"
)

func TestPerformance(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PERFORMANCE TESTS")
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	res := make([]rune, n)
	for i := range res {
		res[i] = letters[rand.Intn(len(letters))]
	}
	return string(res)
}

var _ = Describe("PERFORMANCE TESTS", func() {
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

	localDir := "/tmp/doc/"
	remoteDir := "/doc/"

	It("Client APIs", func() {
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

		experiment := gmeasure.NewExperiment("Client APIs")
		AddReportEntry(experiment.Name, experiment)

		c := client.NewClient(false)
		defer c.CloseClient()

		err := c.Mkdir(remoteDir)
		Expect(err).To(BeNil())

		err = os.MkdirAll(localDir, os.ModePerm)
		Expect(err).To(BeNil())

		// we sample a function repeatedly to get a statistically significant set of measurements
		experiment.Sample(func(idx int) {
			suffix := randomString(32)

			tempFile, err := os.CreateTemp("", suffix)
			Expect(err).To(BeNil())
			_, err = tempFile.WriteString(randomString(1024 * 1024))
			Expect(err).To(BeNil())
			tempFile.Close()

			experiment.MeasureDuration("Put", func() {
				err := c.Put(tempFile.Name(), remoteDir+suffix)
				Expect(err).To(BeNil())
			})

			experiment.MeasureDuration("Get", func() {
				err = c.Get(remoteDir+suffix, localDir+suffix)
				Expect(err).To(BeNil())
			})

			newSuffix := randomString(32)

			experiment.MeasureDuration("Rename", func() {
				err := c.Rename(remoteDir+suffix, remoteDir+newSuffix)
				Expect(err).To(BeNil())
			})

			experiment.MeasureDuration("Stat", func() {
				_, err := c.Stat(remoteDir + newSuffix)
				Expect(err).To(BeNil())
			})

			experiment.MeasureDuration("List", func() {
				_, err := c.List(remoteDir)
				Expect(err).To(BeNil())
			})
		}, gmeasure.SamplingConfig{N: 10, Duration: time.Minute})
		// we'll sample the function up to 10 times or up to a minute, whichever comes first
	})
})
