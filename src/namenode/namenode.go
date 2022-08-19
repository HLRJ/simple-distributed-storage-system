package namenode

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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

	fileToUUIDs        map[string][]uuid.UUID
	uuidToDataNodeLocs map[uuid.UUID][]int
}

func (s *nameNodeServer) GetBlockAddrs(ctx context.Context, in *protos.GetBlockAddrsRequest) (*protos.GetBlockAddrsReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// get uuids
	uuids, ok := s.fileToUUIDs[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("file %v not exists", in.Path))
	}

	if uint64(len(uuids)) <= in.Index {
		return nil, errors.New(fmt.Sprintf("index %v out of range %v", in.Index, len(uuids)))
	}

	// get uuid
	id := uuids[in.Index]
	bin, err := id.MarshalBinary()
	if err != nil {
		return nil, err
	}

	// get locs
	locs, ok := s.uuidToDataNodeLocs[id]
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
		Uuid:  bin,
	}, nil
}

func (s *nameNodeServer) RegisterDataNode(ctx context.Context, in *protos.RegisterDataNodeRequest) (*protos.RegisterDataNodeReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	loc, ok := s.dataNodeAddrToLoc[in.Address]
	if ok {
		// delete outdated datanode server
		s.removeDataNodeServer(loc, in.Address)
	}

	// update addr <-> loc
	s.dataNodeAddrToLoc[in.Address] = s.dataNodeMaxLoc
	s.dataNodeLocToAddr[s.dataNodeMaxLoc] = in.Address

	log.Infof("namenode server %v register datanode server %v with loc %v",
		consts.NameNodeServerAddr, in.Address, s.dataNodeMaxLoc)

	s.dataNodeMaxLoc++

	return &protos.RegisterDataNodeReply{BlockSize: blockSize}, nil
}

func (s *nameNodeServer) Create(ctx context.Context, in *protos.CreateRequest) (*protos.CreateReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof("namenode server %v create file %v", consts.NameNodeServerAddr, in.Path)

	// check file existence
	_, ok := s.fileToUUIDs[in.Path]
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
	s.fileToUUIDs[in.Path] = uuids

	// alloc locs for uuid
	for _, id := range uuids {
		locs, err := utils.RandomChooseLocs(s.fetchAllLocs(), replicaFactor)
		if err != nil {
			return nil, err
		}
		s.uuidToDataNodeLocs[id] = locs
		log.Infof("uuid %v -> locs %v", id, locs)
	}

	return &protos.CreateReply{BlockSize: blockSize}, nil
}

func (s *nameNodeServer) Open(ctx context.Context, in *protos.OpenRequest) (*protos.OpenReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof("namenode server %v open file %v", consts.NameNodeServerAddr, in.Path)

	// check file existence
	uuids, ok := s.fileToUUIDs[in.Path]
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

func (s *nameNodeServer) removeDataNodeServer(loc int, addr string) {
	log.Infof("namenode server %v remove datanode server %v with loc %v", consts.NameNodeServerAddr, addr, loc)

	// start data migration
	if s.dataMigration(loc) {
		// delete addr <-> loc
		delete(s.dataNodeLocToAddr, loc)
		delete(s.dataNodeAddrToLoc, addr)
	}
}

func (s *nameNodeServer) dataMigration(loc int) bool {
	log.Infof("namenode server %v start data migration for loc %v", consts.NameNodeServerAddr, loc)
	failed := false

	for id, locs := range s.uuidToDataNodeLocs {
		index := utils.ContainsLoc(locs, loc)
		if index != -1 {
			log.Infof("uuid %v -> locs %v contains %v", id, locs, loc)

			// fetch candidates
			var candidates []int
			for candidate := range s.fetchAllLocs() {
				if utils.ContainsLoc(locs, candidate) == -1 && candidate != loc {
					candidates = append(candidates, candidate)
				}
			}

			log.Infof("uuid %v -> fetch candidates %v", id, candidates)

			// random choose one
			res, err := utils.RandomChooseLocs(candidates, 1)
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}

			fromLoc := locs[(index+1)%len(locs)]
			toLoc := res[0]

			// get addr
			fromAddr, ok := s.dataNodeLocToAddr[fromLoc]
			if !ok {
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}
			toAddr, ok := s.dataNodeLocToAddr[toLoc]
			if !ok {
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}

			log.Infof("uuid %v -> copy from %v to %v", id, fromAddr, toAddr)

			// connect to datanode server and read data
			datanode, conn := utils.ConnectToDataNode(fromAddr)
			bin, err := id.MarshalBinary()
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}
			reply, err := datanode.Read(context.Background(), &protos.ReadRequest{Uuid: bin})
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}
			err = conn.Close()
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}

			// connect to datanode server and write data
			datanode, conn = utils.ConnectToDataNode(toAddr)
			_, err = datanode.Write(context.Background(), &protos.WriteRequest{Uuid: bin, Data: reply.Data})
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}
			err = conn.Close()
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}

			// modify UUIDToDataNodeLocs
			locs = append(locs[:index], locs[index:]...)
			locs = append(locs, toLoc)
			s.uuidToDataNodeLocs[id] = locs // TODO: iterator maybe corrupted
		}
	}

	return !failed
}

func (s *nameNodeServer) startHeartbeatTicker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("namenode server %v stop heartbeat", consts.NameNodeServerAddr)
			return
		case <-time.After(5 * time.Second):
			s.mu.Lock()
			log.Infof("namenode server %v start heartbeat ticker", consts.NameNodeServerAddr)
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
						s.removeDataNodeServer(loc, addr)
					}
					err = conn.Close()
					if err != nil {
						log.Warn(err)
					}
				}
			}
			s.mu.Unlock()
		}
	}
}

func NewNameNodeServer() *nameNodeServer {
	return &nameNodeServer{
		dataNodeMaxLoc:     0,
		dataNodeLocToAddr:  make(map[int]string),
		dataNodeAddrToLoc:  make(map[string]int),
		fileToUUIDs:        make(map[string][]uuid.UUID),
		uuidToDataNodeLocs: make(map[uuid.UUID][]int),
	}
}

func (s *nameNodeServer) Setup(ctx context.Context) {
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
	go s.startHeartbeatTicker(ctx)

	// blocked here
	select {
	case <-ctx.Done():
		server.Stop()
		log.Infof("namenode server %v quitting", consts.NameNodeServerAddr)
		return
	}
}
