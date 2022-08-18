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
	blockSize     uint64 = 64
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

	// get uuids
	uuids, ok := s.FileToUUIDs[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("file %v not exists", in.Path))
	}

	if uint64(len(uuids)) <= in.Index {
		return nil, errors.New(fmt.Sprintf("index %v out of range %v", in.Index, len(uuids)))
	}

	// get uuid
	id := uuids[in.Index]
	res, err := id.MarshalBinary()
	if err != nil {
		return nil, err
	}

	// get locs
	locs, ok := s.UUIDToDataNodeLocs[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("uuid %v not exists", id))
	}

	// get addrs
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
		// delete outdated datanode server
		delete(s.dataNodeAddrToLoc, in.Address)
		delete(s.dataNodeLocToAddr, loc)
	}

	// update addr <-> loc
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

	// check file existence
	_, ok := s.FileToUUIDs[in.Path]
	if ok {
		return nil, errors.New(fmt.Sprintf("file %v already exists", in.Path))
	}

	// calculate blocks and assign uuids
	var uuids []uuid.UUID
	blocks := utils.CeilDiv(in.Size, blockSize)
	for i := 0; i < blocks; i++ {
		id := uuid.New()
		uuids = append(uuids, id)
		log.Infof("block #%v -> uuid %v", i, id)
	}
	s.FileToUUIDs[in.Path] = uuids

	// alloc locs for uuid
	for _, id := range uuids {
		locs, err := s.randomChooseLocs(replicaFactor)
		if err != nil {
			return nil, err
		}
		s.UUIDToDataNodeLocs[id] = locs
		log.Infof("uuid %v -> locs %v", id, locs)
	}

	return &protos.CreateReply{BlockSize: blockSize}, nil
}

func (s *nameNodeServer) Open(ctx context.Context, in *protos.OpenRequest) (*protos.OpenReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof("namenode server %v open file %v", consts.NameNodeServerAddr, in.Path)

	// check file existence
	uuids, ok := s.FileToUUIDs[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("file %v not exists", in.Path))
	}

	// return blocks
	return &protos.OpenReply{BlockSize: blockSize, Blocks: uint64(len(uuids))}, nil
}

func (s *nameNodeServer) fetchAllLocs() []int {
	index := 0
	locs := make([]int, len(s.dataNodeLocToAddr))
	for loc := range s.dataNodeLocToAddr {
		locs[index] = loc
		index++
	}
	return locs
}

func (s *nameNodeServer) randomChooseLocs(count int) ([]int, error) {
	locs := s.fetchAllLocs()

	if len(locs) < count {
		return nil, errors.New("insufficient datanode server")
	}

	rand.Seed(time.Now().Unix())
	rand.Shuffle(len(locs), func(i int, j int) {
		locs[i], locs[j] = locs[j], locs[i]
	})

	result := make([]int, 0, count)
	for index, value := range locs {
		if index == count {
			break
		}
		result = append(result, value)
	}
	return result, nil
}

func (s *nameNodeServer) startHeartbeatTicker() {
	for {
		s.mu.Lock()
		locs := s.fetchAllLocs()
		for _, loc := range locs {
			addr, ok := s.dataNodeLocToAddr[loc]
			if ok {
				datanode, conn := utils.ConnectToDataNode(addr)
				_, err := datanode.HeartBeat(context.Background(), &protos.HeartBeatRequest{})
				if err != nil {
					// delete unreachable datanode server
					log.Warn(err)
					log.Infof("namenode server %v find datanode %v unreachable", consts.NameNodeServerAddr, addr)
					delete(s.dataNodeLocToAddr, loc)
					delete(s.dataNodeAddrToLoc, addr)
				}
				err = conn.Close()
				if err != nil {
					log.Warn(err)
				}
			}
		}
		s.mu.Unlock()
		time.Sleep(10 * time.Second)
	}
}

func NewNameNodeServer() *nameNodeServer {
	return &nameNodeServer{
		dataNodeMaxLoc:     0,
		dataNodeLocToAddr:  make(map[int]string),
		dataNodeAddrToLoc:  make(map[string]int),
		FileToUUIDs:        make(map[string][]uuid.UUID),
		UUIDToDataNodeLocs: make(map[uuid.UUID][]int),
	}
}

func (s *nameNodeServer) Setup() {
	// setup namenode server
	log.Infof("starting namenode server at %v", consts.NameNodeServerAddr)
	listener, err := net.Listen("tcp", consts.NameNodeServerAddr)
	if err != nil {
		log.Panic(err)
	}
	server := grpc.NewServer()
	protos.RegisterNameNodeServer(server, s)

	go func() {
		err = server.Serve(listener)
		if err != nil {
			log.Panic(err)
		}
	}()

	// start heartbeat ticker
	go s.startHeartbeatTicker()

	// blocked here
	select {}
}
