package utils

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math"
	"math/rand"
	"simple-distributed-storage-system/src/consts"
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

func ConnectToDataNode(addr string) (protos.DataNodeClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Warn(err)
		return nil, nil, err
	}
	return protos.NewDataNodeClient(conn), conn, nil
}

func ConnectToNameNode() (protos.NameNodeClient, *grpc.ClientConn, error) {
	for _, addr := range consts.NameNodeServerAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Warn(err)
			continue
		}
		nameNode := protos.NewNameNodeClient(conn)
		_, err = nameNode.IsLeader(context.Background(), &protos.IsLeaderRequest{})
		if err != nil {
			log.Warn(err)
			continue
		}
		return nameNode, conn, nil
	}
	return nil, nil, errors.New("no available namenode server")
}
