package datanode

import (
	"simple-distributed-storage-system/src/protos"
)

type datanodeServer struct {
	protos.UnimplementedDataNodeServer
	addr        string
	blockSize   uint64
	blockNumber uint64
}

func (s *datanodeServer) localFileSystemRoot() string {
	return "/tmp/gfs/chunks/" + s.addr + "/"
}
