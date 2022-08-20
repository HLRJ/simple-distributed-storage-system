package datanode

import (
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"os"
	"simple-distributed-storage-system/src/protos"
	"simple-distributed-storage-system/src/utils"
	"time"
)

func NewDataNodeServer(addr string) *dataNodeServer {
	return &dataNodeServer{
		addr: addr,
	}
}

func (s *dataNodeServer) Setup(ctx context.Context) {
	// create local fs
	err := os.MkdirAll(s.localFileSystemRoot(), os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	// setup datanode server
	log.Infof("starting datanode server %v", s.addr)
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Panic(err)
	}
	server := grpc.NewServer()
	protos.RegisterDataNodeServer(server, s)

	go func() {
		err = server.Serve(listener)
		if err != nil {
			log.Panic(err)
		}
	}()

	// connect to namenode
retry:
	nameNode, conn, err := utils.ConnectToNameNode(false)
	if err != nil {
		log.Panic(err)
	}
	reply, err := nameNode.RegisterDataNode(context.Background(), &protos.RegisterDataNodeRequest{Address: s.addr})
	if err != nil {
		// TODO: max retries
		conn.Close()
		log.Warn(err)
		time.Sleep(time.Second)
		goto retry
	}

	// set block size
	s.blockSize = reply.BlockSize

	// blocked here
	select {
	case <-ctx.Done():
		server.Stop()
		log.Infof("datanode server %v quitting", s.addr)
		return
	}
}
