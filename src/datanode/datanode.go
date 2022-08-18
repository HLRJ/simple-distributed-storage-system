package datanode

import (
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"
)

type dataNodeServer struct {
	protos.DataNodeServer
}

func newDataNodeServer() *dataNodeServer {
	return &dataNodeServer{}
}

func Init(address string) {
	// setup datanode server
	log.Infof("starting datanode server at %v", address)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Panic(err)
	}
	server := grpc.NewServer()
	protos.RegisterDataNodeServer(server, newDataNodeServer())

	go func() {
		err = server.Serve(listener)
		if err != nil {
			log.Panic(err)
		}
	}()

	// connect to namenode
	conn, err := grpc.Dial(consts.NameNodeServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Panic(err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Panic(err)
		}
	}(conn)

	client := protos.NewNameNodeClient(conn)
	_, err = client.RegisterDataNode(context.Background(), &protos.RegisterDataNodeRequest{Address: address})
	if err != nil {
		log.Panic(err)
	}

	select {}
}
