package utils

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math"
	"math/rand"
	"simple-distributed-storage-system/src/protos"
	"time"
)

func CeilDiv(a, b uint64) int {
	return int(math.Ceil(float64(a) / float64(b)))
}

func Min(a, b uint64) int {
	if a < b {
		return int(a)
	} else {
		return int(b)
	}
}

func ConnectToDataNode(addr string) (protos.DataNodeClient, *grpc.ClientConn) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Panic(err)
	}

	return protos.NewDataNodeClient(conn), conn
}

func ContainsLoc(locs []int, loc int) int {
	for index, eachLoc := range locs {
		if eachLoc == loc {
			return index
		}
	}
	return -1
}

func RandomChooseLocs(locs []int, count int) ([]int, error) {
	if len(locs) < count {
		return nil, errors.New("insufficient locs")
	}

	rand.Seed(time.Now().Unix())
	rand.Shuffle(len(locs), func(i int, j int) {
		locs[i], locs[j] = locs[j], locs[i]
	})

	result := make([]int, 0, count)
	for index, value := range locs {
		if index == count {
			break
		}
		result = append(result, value)
	}
	return result, nil
}
