package datanode

import (
	"simple-distributed-storage-system/src/protos"
)

type dataNodeServer struct {
	protos.UnimplementedDataNodeServer
	addr        string
	blockSize   uint64
	blockNumber uint64
}

func (s *dataNodeServer) localFileSystemRoot() string {
	return "/tmp/gfs/chunks/" + s.addr + "/"
}
