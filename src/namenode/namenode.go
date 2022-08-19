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
	"strings"
	"sync"
	"time"
)

const (
	replicaFactor                    = 3
	nameNodeHeartbeatDuration        = 5
	blockSize                 uint64 = 64
)

type fileInfo struct {
	ids  []uuid.UUID
	size uint64
}

type nameNodeServer struct {
	protos.UnimplementedNameNodeServer
	mu sync.Mutex

	dataNodeMaxLoc    int
	dataNodeLocToAddr map[int]string
	dataNodeAddrToLoc map[string]int

	fileToInfo             map[string]fileInfo
	uuidToDataNodeLocsInfo map[uuid.UUID]map[int]bool

	registrationContext bool
	registrationAddr    string
}

func (s *nameNodeServer) GetBlockAddrs(ctx context.Context, in *protos.GetBlockAddrsRequest) (*protos.GetBlockAddrsReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// get uuids
	info, ok := s.fileToInfo[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("file %v not exists", in.Path))
	}

	if uint64(len(info.ids)) <= in.Index {
		return nil, errors.New(fmt.Sprintf("index %v out of range %v", in.Index, len(info.ids)))
	}

	// get uuid
	id := info.ids[in.Index]
	bin, err := id.MarshalBinary()
	if err != nil {
		return nil, err
	}

	// get locs
	locsInfo, ok := s.uuidToDataNodeLocsInfo[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("uuid %v not exists", id))
	}

	// get addrs
	var addrs []string
	for loc, ok := range locsInfo {
		switch in.Type {
		// existed
		case protos.GetBlockAddrsRequestType_OP_REMOVE:
			delete(s.fileToInfo, in.Path)
			fallthrough
		case protos.GetBlockAddrsRequestType_OP_GET:
			if ok {
				addr, ok := s.dataNodeLocToAddr[loc]
				if ok {
					addrs = append(addrs, addr)
				}
			}

		// not existed
		case protos.GetBlockAddrsRequestType_OP_PUT:
			if !ok {
				addr, ok := s.dataNodeLocToAddr[loc]
				if ok {
					addrs = append(addrs, addr)
				}
			}
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
	s.registrationContext = true
	s.registrationAddr = in.Address
	defer func() {
		s.registrationAddr = ""
		s.registrationContext = false
		s.mu.Unlock()
	}()

	targetLoc := s.dataNodeMaxLoc
	log.Infof("namenode server %v trying to register datanode server %v with loc %v",
		consts.NameNodeServerAddr, in.Address, targetLoc)

	loc, ok := s.dataNodeAddrToLoc[in.Address]
	if ok {
		// delete outdated datanode server
		if !s.removeDataNodeServer(loc, in.Address) { // active remove
			log.Warnf("namenode server %v cannot register datanode server %v with loc %v",
				consts.NameNodeServerAddr, in.Address, targetLoc)
			return nil, errors.New("registration failure")
		}
	}

	// update addr <-> loc
	s.dataNodeAddrToLoc[in.Address] = targetLoc
	s.dataNodeLocToAddr[targetLoc] = in.Address

	log.Infof("namenode server %v successfully registering datanode server %v with loc %v",
		consts.NameNodeServerAddr, in.Address, targetLoc)

	// increase max loc
	s.dataNodeMaxLoc++

	return &protos.RegisterDataNodeReply{BlockSize: blockSize}, nil
}

func (s *nameNodeServer) Create(ctx context.Context, in *protos.CreateRequest) (*protos.CreateReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof("namenode server %v create file %v", consts.NameNodeServerAddr, in.Path)

	// check file existence
	_, ok := s.fileToInfo[in.Path]
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
	s.fileToInfo[in.Path] = fileInfo{
		ids:  uuids,
		size: in.Size,
	}

	// alloc locs for uuid
	for _, id := range uuids {
		locs, err := utils.RandomChooseLocs(s.fetchAllLocs(), replicaFactor)
		if err != nil {
			return nil, err
		}

		locsInfo := make(map[int]bool)
		for _, locs := range locs {
			locsInfo[locs] = false // invalid now
		}
		s.uuidToDataNodeLocsInfo[id] = locsInfo

		log.Infof("uuid %v -> locs %v", id, locs)
	}

	return &protos.CreateReply{BlockSize: blockSize}, nil
}

func (s *nameNodeServer) Open(ctx context.Context, in *protos.OpenRequest) (*protos.OpenReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof("namenode server %v open file %v", consts.NameNodeServerAddr, in.Path)

	// check file existence
	info, ok := s.fileToInfo[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("file %v not exists", in.Path))
	}

	// return blocks
	return &protos.OpenReply{BlockSize: blockSize, Blocks: uint64(len(info.ids))}, nil
}

func (s *nameNodeServer) LocsValidityNotify(ctx context.Context, in *protos.LocsValidityNotifyRequest) (*protos.LocsValidityNotifyReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := uuid.New()
	err := id.UnmarshalBinary(in.Uuid)
	if err != nil {
		log.Panic(err)
	}

	log.Infof("namenode server %v receive locs validity %v for uuid %v", consts.NameNodeServerAddr, in.Validity, id)

	locsInfo, ok := s.uuidToDataNodeLocsInfo[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("uuid %v not exists", id))
	}

	for addr, validity := range in.Validity {
		loc, ok := s.dataNodeAddrToLoc[addr]
		if !ok {
			log.Warnf("addr %v not exists", addr)
		} else {
			_, ok := locsInfo[loc]
			if !ok {
				log.Warnf("loc %v not exists", addr)
			} else {
				s.uuidToDataNodeLocsInfo[id][loc] = validity
			}
		}
	}

	return &protos.LocsValidityNotifyReply{}, nil
}

func (s *nameNodeServer) FetchFileInfo(ctx context.Context, in *protos.FetchFileInfoRequest) (*protos.FetchFileInfoReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof("namenode server %v stat path %v", consts.NameNodeServerAddr, in.Path)

	// check file existence
	info, ok := s.fileToInfo[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("path %v not exists", in.Path))
	}

	var infos []*protos.FileInfo
	if in.Path[len(in.Path)-1] != '/' { // is file
		infos = append(infos, &protos.FileInfo{
			Name: in.Path,
			Size: info.size,
		})
	} else { // is directory
		for file, info := range s.fileToInfo {
			if strings.HasPrefix(file, in.Path) {
				infos = append(infos, &protos.FileInfo{
					Name: file,
					Size: info.size,
				})
			}
		}
	}

	return &protos.FetchFileInfoReply{Infos: infos}, nil
}

func (s *nameNodeServer) fetchAllLocs() []int {
	index := 0
	locs := make([]int, len(s.dataNodeLocToAddr))
	for loc := range s.dataNodeLocToAddr {
		locs[index] = loc
		index++
	}
	if s.registrationContext {
		locs = append(locs, s.dataNodeMaxLoc)
	}
	return locs
}

func (s *nameNodeServer) removeDataNodeServer(loc int, addr string) bool {
	log.Infof("namenode server %v trying to remove datanode server %v with loc %v", consts.NameNodeServerAddr, addr, loc)

	// start data migration
	if s.dataMigration(loc) {
		// delete addr <-> loc
		delete(s.dataNodeLocToAddr, loc)
		delete(s.dataNodeAddrToLoc, addr)

		log.Infof("namenode server %v successfully removing datanode server %v with loc %v", consts.NameNodeServerAddr, addr, loc)
		return true
	}

	log.Warnf("namenode server %v cannot remove datanode server %v with loc %v", consts.NameNodeServerAddr, addr, loc)
	return false
}

func (s *nameNodeServer) dataMigration(loc int) bool {
	log.Infof("namenode server %v start data migration for loc %v", consts.NameNodeServerAddr, loc)
	failed := false

	for id, locsInfo := range s.uuidToDataNodeLocsInfo {
		valid, ok := locsInfo[loc]
		if ok && valid {
			log.Infof("uuid %v -> locs %v contains %v", id, locsInfo, loc)

			// fetch candidates as to
			var candidates []int
			for candidate := range s.fetchAllLocs() {
				_, ok := locsInfo[candidate]
				if !ok {
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

			// get addr
			toLoc := res[0]
			var toAddr string
			if toLoc == s.dataNodeMaxLoc {
				toAddr = s.registrationAddr
			} else {
				toAddr, ok = s.dataNodeLocToAddr[toLoc]
				if !ok {
					log.Warnf("unable to migrate data for %v", id)
					failed = true
					break
				}
			}

			// fetch candidates as from
			fromLoc := -1
			for candidate, valid := range locsInfo {
				if valid && candidate != loc {
					fromLoc = candidate
					break
				}
			}
			if fromLoc == -1 {
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}

			// get addr
			fromAddr, ok := s.dataNodeLocToAddr[fromLoc]
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

			// modify uuidToDataNodeLocsInfo
			delete(s.uuidToDataNodeLocsInfo[id], loc)
			s.uuidToDataNodeLocsInfo[id][toLoc] = true
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
		case <-time.After(nameNodeHeartbeatDuration * time.Second):
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
						s.removeDataNodeServer(loc, addr) // passive remove
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
		dataNodeLocToAddr:      make(map[int]string),
		dataNodeAddrToLoc:      make(map[string]int),
		fileToInfo:             make(map[string]fileInfo),
		uuidToDataNodeLocsInfo: make(map[uuid.UUID]map[int]bool),
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
