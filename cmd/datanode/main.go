package main

import (
	"log"
	"net"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/datanode"
	"simple-distributed-storage-system/src/protos"

	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", consts.DataNodeServerAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	DataServer := datanode.MakeServer()
	protos.RegisterDataserverServer(s, DataServer)
	s.Serve(lis)
}
