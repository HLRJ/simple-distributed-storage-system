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

	localPath := "../../LICENSE"
	localCopyPath := "/tmp/LICENSE"
	remoteDir := "/doc/"
	remotePathWithDir := "/doc/LICENSE"

	It("Client APIs", func() {
		ctx, cancelFunc := context.WithCancel(context.Background())

		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[0], 1).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[1], 2).Setup(ctx)
		go namenode.NewNameNodeServer(consts.NameNodeServerAddrs[2], 3).Setup(ctx)
		time.Sleep(5 * time.Second)

		go datanode.NewDataNodeServer("localhost:9000").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9001").Setup(ctx)
		go datanode.NewDataNodeServer("localhost:9002").Setup(ctx)
		time.Sleep(5 * time.Second)

		experiment := gmeasure.NewExperiment("Client APIs")
		AddReportEntry(experiment.Name, experiment)

		c := client.NewClient(false)

		err := c.Mkdir(remoteDir)
		Expect(err).To(BeNil())
		err = c.Put(localPath, remotePathWithDir)
		Expect(err).To(BeNil())

		// we sample a function repeatedly to get a statistically significant set of measurements
		experiment.Sample(func(idx int) {
			experiment.MeasureDuration("Get", func() {
				err = c.Get(remotePathWithDir, localCopyPath)
				Expect(err).To(BeNil())
			})
		}, gmeasure.SamplingConfig{N: 20, Duration: time.Minute})
		// we'll sample the function up to 20 times or up to a minute, whichever comes first

		experiment.Sample(func(idx int) {
			experiment.MeasureDuration("Stat", func() {
				_, err := c.Stat(remotePathWithDir)
				Expect(err).To(BeNil())
			})
		}, gmeasure.SamplingConfig{N: 20, Duration: time.Minute})

		experiment.Sample(func(idx int) {
			experiment.MeasureDuration("List", func() {
				_, err := c.List(remoteDir)
				Expect(err).To(BeNil())
			})
		}, gmeasure.SamplingConfig{N: 20, Duration: time.Minute})

		experiment.Sample(func(idx int) {
			suffix := randomString(16)

			tempFile, err := os.CreateTemp("", suffix)
			Expect(err).To(BeNil())
			_, err = tempFile.WriteString(randomString(256))
			Expect(err).To(BeNil())
			tempFile.Close()

			experiment.MeasureDuration("Put", func() {
				err := c.Put(tempFile.Name(), remoteDir+suffix)
				Expect(err).To(BeNil())
			})

			newSuffix := randomString(16)

			experiment.MeasureDuration("Rename", func() {
				err := c.Rename(remoteDir+suffix, remoteDir+newSuffix)
				Expect(err).To(BeNil())
			})
		}, gmeasure.SamplingConfig{N: 20, Duration: time.Minute})

		c.CloseClient()
		cancelFunc()
	})
})
