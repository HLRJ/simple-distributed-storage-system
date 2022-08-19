package main

import (
	"flag"
	"net"
	"simple-distributed-storage-system/src/datanode"
	"simple-distributed-storage-system/src/protos"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"
)

var dataNodeServerAddr = flag.String("addr", "localhost:9000", "input this datanode address")

func main() {

	flag.Parse()

	log.Infof("start to listen: %v", *dataNodeServerAddr)
	lis, err := net.Listen("tcp", *dataNodeServerAddr)
	if err != nil {
		log.Infof("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	DataServer := datanode.MakeServer()
	protos.RegisterDataNodeServer(s, DataServer)
	errServe := s.Serve(lis)
	if errServe != nil {
		log.Panic(errServe)
	}
}
