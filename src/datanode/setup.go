package datanode

import (
	"context"
	zipkingrpc "github.com/openzipkin/zipkin-go/middleware/grpc"
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
		addr:        addr,
		blockNumber: 0,
	}
}

func (s *dataNodeServer) Setup(ctx context.Context) {
	// create local fs
	err := os.MkdirAll(s.localFileSystemRoot(), os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	//zipkin
	tracer, r, err := utils.NewZipkinTracer(utils.ZIPKIN_HTTP_ENDPOINT, "dataNodeServer", s.addr)
	defer r.Close()
	if err != nil {
		log.Println(err)
		return
	}
	// setup datanode server
	log.Infof("starting datanode server %v", s.addr)
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Panic(err)
	}

	server := grpc.NewServer(grpc.StatsHandler(zipkingrpc.NewServerHandler(tracer)))
	protos.RegisterDataNodeServer(server, s)

	go func() {
		err = server.Serve(listener)
		if err != nil {
			log.Panic(err)
		}
	}()

	// connect to namenode
retry:
	retries := 0
	namenode, conn, err := utils.ConnectToNameNode(false)
	if err != nil {
		log.Panic(err)
	}
	reply, err := namenode.RegisterDataNode(context.Background(), &protos.RegisterDataNodeRequest{Address: s.addr})
	if err != nil {
		conn.Close()
		log.Warn(err)
		time.Sleep(time.Second)
		retries++
		if retries >= 8 {
			log.Panicf("datanode server %v cannot register in namenode server", s.addr)
		}
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
