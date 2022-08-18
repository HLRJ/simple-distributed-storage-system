package namenode

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"math/rand"
	"net"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"
	"simple-distributed-storage-system/src/utils"
	"sync"
	"time"
)

const (
	replicaFactor        = 3
	blockSize     uint64 = 1024
)

type nameNodeServer struct {
	protos.UnimplementedNameNodeServer
	mu sync.Mutex

	dataNodeMaxLoc    int
	dataNodeLocToAddr map[int]string
	dataNodeAddrToLoc map[string]int

	FileToUUIDs        map[string][]uuid.UUID
	UUIDToDataNodeLocs map[uuid.UUID][]int
}

func (s *nameNodeServer) GetBlockAddrs(ctx context.Context, in *protos.GetBlockAddrsRequest) (*protos.GetBlockAddrsReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	uuids, ok := s.FileToUUIDs[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("file %v not exists", in.Path))
	}

	if uint64(len(uuids)) <= in.Index {
		return nil, errors.New(fmt.Sprintf("index %v out of range %v", in.Index, len(uuids)))
	}

	id := uuids[in.Index]
	res, err := id.MarshalBinary()
	if err != nil {
		return nil, err
	}

	locs, ok := s.UUIDToDataNodeLocs[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("uuid %v not exists", id))
	}

	var addrs []string
	for _, loc := range locs {
		addr, ok := s.dataNodeLocToAddr[loc]
		if ok {
			addrs = append(addrs, addr)
		}
	}

	log.Infof("namenode server %v get addrs %v for file %v at block #%v",
		consts.NameNodeServerAddr, addrs, in.Path, in.Index)

	return &protos.GetBlockAddrsReply{
		Addrs: addrs,
		Uuid:  res,
	}, nil
}

func (s *nameNodeServer) RegisterDataNode(ctx context.Context, in *protos.RegisterDataNodeRequest) (*protos.RegisterDataNodeReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	loc, ok := s.dataNodeAddrToLoc[in.Address]
	if ok {
		delete(s.dataNodeAddrToLoc, in.Address)
		delete(s.dataNodeLocToAddr, loc)
	}

	s.dataNodeAddrToLoc[in.Address] = s.dataNodeMaxLoc
	s.dataNodeLocToAddr[s.dataNodeMaxLoc] = in.Address

	log.Infof("namenode server %v register %v with loc %v",
		consts.NameNodeServerAddr, in.Address, s.dataNodeMaxLoc)

	s.dataNodeMaxLoc++

	return &protos.RegisterDataNodeReply{BlockSize: blockSize}, nil
}

func (s *nameNodeServer) Create(ctx context.Context, in *protos.CreateRequest) (*protos.CreateReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof("namenode server %v create file %v", consts.NameNodeServerAddr, in.Path)

	_, ok := s.FileToUUIDs[in.Path]
	if ok {
		return nil, errors.New(fmt.Sprintf("file %v already exists", in.Path))
	}

	var uuids []uuid.UUID
	blocks := utils.CeilDiv(in.Size, blockSize)
	for i := 0; i < blocks; i++ {
		id := uuid.New()
		uuids = append(uuids, id)
		log.Infof("block #%v -> uuid %v", i, id)
	}
	s.FileToUUIDs[in.Path] = uuids

	for _, id := range uuids {
		locs, err := s.fetchDataNodeLocs()
		if err != nil {
			return nil, err
		}
		s.UUIDToDataNodeLocs[id] = locs
		log.Infof("uuid %v -> locs %v", id, locs)
	}

	return &protos.CreateReply{BlockSize: blockSize}, nil
}

func (s *nameNodeServer) fetchDataNodeLocs() ([]int, error) {
	origin := make([]int, len(s.dataNodeLocToAddr))
	for loc := range s.dataNodeLocToAddr {
		origin = append(origin, loc)
	}

	if len(origin) < replicaFactor {
		return nil, errors.New("insufficient datanode server")
	}

	rand.Seed(time.Now().Unix())
	rand.Shuffle(len(origin), func(i int, j int) {
		origin[i], origin[j] = origin[j], origin[i]
	})

	result := make([]int, 0, replicaFactor)
	for index, value := range origin {
		if index == replicaFactor {
			break
		}
		result = append(result, value)
	}
	return result, nil
}

func newNameNodeServer() *nameNodeServer {
	return &nameNodeServer{
		dataNodeMaxLoc:     0,
		dataNodeLocToAddr:  make(map[int]string),
		dataNodeAddrToLoc:  make(map[string]int),
		FileToUUIDs:        make(map[string][]uuid.UUID),
		UUIDToDataNodeLocs: make(map[uuid.UUID][]int),
	}
}

func Setup() {
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
