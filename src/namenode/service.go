package namenode

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"simple-distributed-storage-system/src/protos"
	"simple-distributed-storage-system/src/utils"
	"strings"
)

func (s *nameNodeServer) GetBlockAddrs(ctx context.Context, in *protos.GetBlockAddrsRequest) (*protos.GetBlockAddrsReply, error) {
	if !s.isLeader() && in.Type == protos.GetBlockAddrsRequestType_OP_REMOVE {
		return nil, errors.New(fmt.Sprintf("namenode server %v is not leader", s.addr))
	}
	s.mu.Lock()
	defer func() {
		if in.Type == protos.GetBlockAddrsRequestType_OP_REMOVE {
			s.syncPropose()
		}
		s.mu.Unlock()
	}()

	// get uuids
	info, ok := s.sm.FileToInfo[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("file %v not exists", in.Path))
	}

	if uint64(len(info.Ids)) <= in.Index {
		return nil, errors.New(fmt.Sprintf("index %v out of range %v", in.Index, len(info.Ids)))
	}

	// get uuid
	id := info.Ids[in.Index]
	bin, err := id.MarshalBinary()
	if err != nil {
		return nil, err
	}

	// get locs
	locsInfo, ok := s.sm.UUIDToDataNodeLocsInfo[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("uuid %v not exists", id))
	}

	// get addrs
	var addrs []string
	for loc, ok := range locsInfo {
		switch in.Type {
		// existed
		case protos.GetBlockAddrsRequestType_OP_REMOVE:
			delete(s.sm.FileToInfo, in.Path) // modify state
			fallthrough
		case protos.GetBlockAddrsRequestType_OP_GET:
			if ok {
				addr, ok := s.sm.DataNodeLocToAddr[loc]
				if ok {
					addrs = append(addrs, addr)
				}
			}

		// not existed
		case protos.GetBlockAddrsRequestType_OP_PUT:
			if !ok {
				addr, ok := s.sm.DataNodeLocToAddr[loc]
				if ok {
					addrs = append(addrs, addr)
				}
			}
		}

	}

	log.Infof("namenode server %v get addrs %v for file %v at block #%v",
		s.addr, addrs, in.Path, in.Index)

	return &protos.GetBlockAddrsReply{
		Addrs: addrs,
		Uuid:  bin,
	}, nil
}

func (s *nameNodeServer) RegisterDataNode(ctx context.Context, in *protos.RegisterDataNodeRequest) (*protos.RegisterDataNodeReply, error) {
	if !s.isLeader() {
		return nil, errors.New(fmt.Sprintf("namenode server %v is not leader", s.addr))
	}
	s.mu.Lock()
	s.registrationInfo = registrationInfo{
		context: true,
		addr:    in.Address,
	}
	defer func() {
		s.registrationInfo = registrationInfo{
			context: false,
			addr:    "",
		}
		s.syncPropose()
		s.mu.Unlock()
	}()

	targetLoc := s.sm.DataNodeMaxLoc
	log.Infof("namenode server %v trying to register datanode server %v with loc %v",
		s.addr, in.Address, targetLoc)

	loc, ok := s.sm.DataNodeAddrToLoc[in.Address]
	if ok {
		// delete outdated datanode server
		if !s.removeDataNodeServer(loc, in.Address) { // active remove
			log.Warnf("namenode server %v cannot register datanode server %v with loc %v",
				s.addr, in.Address, targetLoc)
			return nil, errors.New("registration failure")
		}
	}

	// update addr <-> loc
	s.sm.DataNodeAddrToLoc[in.Address] = targetLoc
	s.sm.DataNodeLocToAddr[targetLoc] = in.Address

	log.Infof("namenode server %v successfully registering datanode server %v with loc %v",
		s.addr, in.Address, targetLoc)

	// increase max loc
	s.sm.DataNodeMaxLoc++

	return &protos.RegisterDataNodeReply{BlockSize: blockSize}, nil
}

func (s *nameNodeServer) Create(ctx context.Context, in *protos.CreateRequest) (*protos.CreateReply, error) {
	if !s.isLeader() {
		return nil, errors.New(fmt.Sprintf("namenode server %v is not leader", s.addr))
	}
	s.mu.Lock()
	defer func() {
		s.syncPropose()
		s.mu.Unlock()
	}()

	log.Infof("namenode server %v create file %v", s.addr, in.Path)

	// check file existence
	_, ok := s.sm.FileToInfo[in.Path]
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
	s.sm.FileToInfo[in.Path] = fileInfo{
		Ids:  uuids,
		Size: in.Size,
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
		s.sm.UUIDToDataNodeLocsInfo[id] = locsInfo

		log.Infof("uuid %v -> locs %v", id, locs)
	}

	return &protos.CreateReply{BlockSize: blockSize}, nil
}

func (s *nameNodeServer) Open(ctx context.Context, in *protos.OpenRequest) (*protos.OpenReply, error) {
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
	}()

	log.Infof("namenode server %v open file %v", s.addr, in.Path)

	// check file existence
	info, ok := s.sm.FileToInfo[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("file %v not exists", in.Path))
	}

	// return blocks
	return &protos.OpenReply{BlockSize: blockSize, Blocks: uint64(len(info.Ids))}, nil
}

func (s *nameNodeServer) LocsValidityNotify(ctx context.Context, in *protos.LocsValidityNotifyRequest) (*protos.LocsValidityNotifyReply, error) {
	if !s.isLeader() {
		return nil, errors.New(fmt.Sprintf("namenode server %v is not leader", s.addr))
	}
	s.mu.Lock()
	defer func() {
		s.syncPropose()
		s.mu.Unlock()
	}()

	id := uuid.New()
	err := id.UnmarshalBinary(in.Uuid)
	if err != nil {
		log.Panic(err)
	}

	log.Infof("namenode server %v receive locs validity %v for uuid %v", s.addr, in.Validity, id)

	locsInfo, ok := s.sm.UUIDToDataNodeLocsInfo[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("uuid %v not exists", id))
	}

	for addr, validity := range in.Validity {
		loc, ok := s.sm.DataNodeAddrToLoc[addr]
		if !ok {
			log.Warnf("addr %v not exists", addr)
		} else {
			_, ok := locsInfo[loc]
			if !ok {
				log.Warnf("loc %v not exists", addr)
			} else {
				s.sm.UUIDToDataNodeLocsInfo[id][loc] = validity
			}
		}
	}

	return &protos.LocsValidityNotifyReply{}, nil
}

func (s *nameNodeServer) FetchFileInfo(ctx context.Context, in *protos.FetchFileInfoRequest) (*protos.FetchFileInfoReply, error) {
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
	}()

	log.Infof("namenode server %v stat path %v", s.addr, in.Path)

	// check file existence
	info, ok := s.sm.FileToInfo[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("path %v not exists", in.Path))
	}

	var infos []*protos.FileInfo
	if in.Path[len(in.Path)-1] != '/' { // is file
		infos = append(infos, &protos.FileInfo{
			Name: in.Path,
			Size: info.Size,
		})
	} else { // is directory
		for file, info := range s.sm.FileToInfo {
			if strings.HasPrefix(file, in.Path) {
				infos = append(infos, &protos.FileInfo{
					Name: file,
					Size: info.Size,
				})
			}
		}
	}

	return &protos.FetchFileInfoReply{Infos: infos}, nil
}

func (s *nameNodeServer) Rename(ctx context.Context, in *protos.RenameRequest) (*protos.RenameReply, error) {
	if !s.isLeader() {
		return nil, errors.New(fmt.Sprintf("namenode server %v is not leader", s.addr))
	}
	s.mu.Lock()
	defer func() {
		s.syncPropose()
		s.mu.Unlock()
	}()

	log.Infof("namenode server %v trying to rename %v -> %v", s.addr, in.OldPath, in.NewPath)

	// check file existence
	info, ok := s.sm.FileToInfo[in.OldPath]
	if !ok {
		return nil, errors.New(fmt.Sprintf("path %v not exists", in.OldPath))
	}

	delete(s.sm.FileToInfo, in.OldPath)
	s.sm.FileToInfo[in.NewPath] = info

	return &protos.RenameReply{}, nil
}

func (s *nameNodeServer) IsLeader(ctx context.Context, in *protos.IsLeaderRequest) (*protos.IsLeaderReply, error) {
	if !s.isLeader() {
		return nil, errors.New(fmt.Sprintf("namenode server %v is not leader", s.addr))
	}
	return &protos.IsLeaderReply{}, nil
}
