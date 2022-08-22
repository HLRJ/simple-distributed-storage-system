package utils

import (
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"
	"time"
)

func ConnectToTargetDataNode(addr string) (protos.DataNodeClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return protos.NewDataNodeClient(conn), conn, nil
}

func ConnectToTargetNameNode(addr string, readonly bool) (protos.NameNodeClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	namenode := protos.NewNameNodeClient(conn)
	reply, err := namenode.IsLeader(context.Background(), &protos.IsLeaderRequest{})
	if err != nil {
		// unreachable
		return nil, nil, err
	} else {
		if !reply.Res && !readonly {
			// must connect to leader
			return nil, nil, errors.New(fmt.Sprintf("namenode server %v is not leader", addr))
		}
	}
	return namenode, conn, nil
}

func ConnectToNameNode(readonly bool) (protos.NameNodeClient, *grpc.ClientConn, error) {
	rounds := 0
	for {
		rounds++
		if rounds >= 8 {
			break
		}

		for _, addr := range consts.NameNodeServerAddrs {
			namenode, conn, err := ConnectToTargetNameNode(addr, readonly)
			if err != nil {
				log.Warn(err)
				time.Sleep(time.Second)
				continue
			}

			log.Infof("connect to namenode server %v", addr)
			return namenode, conn, nil
		}
	}

	return nil, nil, errors.New("no available namenode server")
}
