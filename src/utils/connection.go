package utils

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"
)

func ConnectToDataNode(addr string) (protos.DataNodeClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Warn(err)
		return nil, nil, err
	}
	return protos.NewDataNodeClient(conn), conn, nil
}

func ConnectToNameNode(readonly bool) (protos.NameNodeClient, *grpc.ClientConn, error) {
	for _, addr := range consts.NameNodeServerAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Warn(err)
			continue
		}
		nameNode := protos.NewNameNodeClient(conn)
		if !readonly {
			// must connect to leader
			_, err = nameNode.IsLeader(context.Background(), &protos.IsLeaderRequest{})
			if err != nil {
				log.Warn(err)
				continue
			}
		}
		return nameNode, conn, nil
	}
	return nil, nil, errors.New("no available namenode server")
}