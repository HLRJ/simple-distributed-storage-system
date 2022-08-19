package main

import (
	"flag"
	"net"
	"simple-distributed-storage-system/src/datanode"
	"simple-distributed-storage-system/src/protos"
	"strings"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"
)

var nodeId = flag.Int("id", 0, "input this block server node id")
var nodeAddress = flag.String("address", "127.0.0.1:7088,127.0.0.1:7089,127.0.0.1:7090", "node addrs")

func main() {

	blockSvrNodesMap := make(map[int]string)
	nodeAddrsArr := strings.Split(*nodeAddress, ",")
	for i, addr := range nodeAddrsArr {
		blockSvrNodesMap[i] = addr
	}
	log.Infof("start to listen: %v", blockSvrNodesMap[*nodeId])
	lis, err := net.Listen("tcp", blockSvrNodesMap[*nodeId])
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
	log.Info("successfully start nodeServer")
}
