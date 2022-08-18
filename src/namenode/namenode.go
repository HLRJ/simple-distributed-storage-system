package namenode

import (
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"
	"sync"
)

const (
	blockSize   = 64
	invalidAddr = ""
)

type nameNodeServer struct {
	protos.UnimplementedNameNodeServer

	dataNodeLocMu     sync.Mutex
	dataNodeMaxLoc    int
	dataNodeLocToAddr map[int]string
	dataNodeAddrToLoc map[string]int
}

func (s *nameNodeServer) GetBlockSize(ctx context.Context, in *protos.GetBlockSizeRequest) (*protos.GetBlockSizeReply, error) {
	return &protos.GetBlockSizeReply{Size: blockSize}, nil
}

func (s *nameNodeServer) RegisterDataNode(ctx context.Context, in *protos.RegisterDataNodeRequest) (*protos.RegisterDataNodeReply, error) {
	s.dataNodeLocMu.Lock()
	defer s.dataNodeLocMu.Unlock()

	loc, ok := s.dataNodeAddrToLoc[in.Address]
	if ok {
		s.dataNodeLocToAddr[loc] = invalidAddr
	}

	s.dataNodeAddrToLoc[in.Address] = s.dataNodeMaxLoc
	s.dataNodeLocToAddr[s.dataNodeMaxLoc] = in.Address

	log.Infof("namenode server %v apply %v <-> %v", consts.NameNodeServerAddr, in.Address, s.dataNodeMaxLoc)

	s.dataNodeMaxLoc++

	return &protos.RegisterDataNodeReply{}, nil
}

func newNameNodeServer() *nameNodeServer {
	return &nameNodeServer{
		dataNodeMaxLoc:    0,
		dataNodeLocToAddr: make(map[int]string),
		dataNodeAddrToLoc: make(map[string]int),
	}
}

func Init() {
	// setup namenode server
	log.Infof("starting namenode server at %v", consts.NameNodeServerAddr)
	listener, err := net.Listen("tcp", consts.NameNodeServerAddr)
	if err != nil {
		log.Panic(err)
	}
	server := grpc.NewServer()
	protos.RegisterNameNodeServer(server, newNameNodeServer())
	err = server.Serve(listener)
	if err != nil {
		log.Panic(err)
	}
}
