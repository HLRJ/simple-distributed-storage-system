package utils

import (
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math"
	"simple-distributed-storage-system/src/protos"
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
