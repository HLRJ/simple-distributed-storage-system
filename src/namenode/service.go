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

func (s *namenodeServer) FetchBlockAddrs(ctx context.Context, in *protos.FetchBlockAddrsRequest) (*protos.FetchBlockAddrsReply, error) {
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
	}()

	// get uuids
	info, ok := s.state.FileToInfo[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("path %v not exists", in.Path))
	}
	if utils.IsDir(in.Path) {
		return nil, errors.New(fmt.Sprintf("cannot fetch block addr for dir %v", in.Path))
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
	locsInfo, ok := s.state.UUIDToLocs[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("uuid %v not exists", id))
	}

	// get addrs
	var addrs []string
	for loc, ok := range locsInfo {
		switch in.Type {
		// existed
		case protos.FetchBlockAddrsRequestType_OP_REMOVE:
			// lazy remove
			fallthrough
		case protos.FetchBlockAddrsRequestType_OP_GET:
			if ok {
				info, ok := s.state.LocToInfo[loc]
				if ok {
					addrs = append(addrs, info.Addr)
				}
			}

		// not existed
		case protos.FetchBlockAddrsRequestType_OP_PUT:
			if !ok {
				info, ok := s.state.LocToInfo[loc]
				if ok {
					addrs = append(addrs, info.Addr)
				}
			}
		}

	}

	log.Infof("namenode server %v get addrs %v for file %v at block #%v",
		s.addr, addrs, in.Path, in.Index)

	return &protos.FetchBlockAddrsReply{
		Addrs: addrs,
		Uuid:  bin,
	}, nil
}

func (s *namenodeServer) RegisterDataNode(ctx context.Context, in *protos.RegisterDataNodeRequest) (*protos.RegisterDataNodeReply, error) {
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

	targetLoc := s.state.MaxLoc
	log.Infof("namenode server %v trying to register datanode server %v with loc %v",
		s.addr, in.Address, targetLoc)

	loc, err := s.isDataNodeExist(in.Address)
	if err == nil {
		// delete outdated datanode server
		if !s.removeDataNodeServer(loc, in.Address) { // active remove
			log.Warnf("namenode server %v cannot register datanode server %v with loc %v",
				s.addr, in.Address, targetLoc)
			return nil, errors.New("registration failure")
		}
	}

	// update addr <-> loc
	s.state.LocToInfo[targetLoc] = locInfo{
		Addr: in.Address,
	}

	log.Infof("namenode server %v successfully registering datanode server %v with loc %v",
		s.addr, in.Address, targetLoc)

	// increase max loc
	s.state.MaxLoc++

	return &protos.RegisterDataNodeReply{BlockSize: blockSize}, nil
}

func (s *namenodeServer) Create(ctx context.Context, in *protos.CreateRequest) (*protos.CreateReply, error) {
	if !s.isLeader() {
		return nil, errors.New(fmt.Sprintf("namenode server %v is not leader", s.addr))
	}
	s.mu.Lock()
	defer func() {
		s.syncPropose()
		s.mu.Unlock()
	}()

	log.Infof("namenode server %v create path %v", s.addr, in.Path)

	// check path existence
	_, ok := s.state.FileToInfo[in.Path]
	if ok {
		return nil, errors.New(fmt.Sprintf("path %v already exists", in.Path))
	}

	// check dir existence
	var parentDir string
	if !utils.IsDir(in.Path) {
		index := strings.LastIndex(in.Path, "/")
		// include '/'
		parentDir = in.Path[:index+1]
	} else {
		index := strings.LastIndex(in.Path[:len(in.Path)-1], "/") // remove last '/'
		// include '/'
		parentDir = in.Path[:index+1]
	}
	_, ok = s.state.FileToInfo[parentDir]
	if !ok {
		return nil, errors.New(fmt.Sprintf("parent dir %v not exists, create path %v fails", parentDir, in.Path))
	}

	// calculate blocks and assign uuids
	var uuids []uuid.UUID
	blocks := utils.CeilDiv(in.Size, blockSize)
	for i := 0; i < blocks; i++ {
		id := uuid.New()
		uuids = append(uuids, id)
		log.Infof("block #%v -> uuid %v", i, id)
	}
	s.state.FileToInfo[in.Path] = fileInfo{
		Ids:  uuids,
		Size: in.Size,
	}

	// alloc locs for uuid
	for _, id := range uuids {
		locs, err := s.fetchLocs(s.fetchAllLocs(), replicaFactor)
		if err != nil {
			return nil, err
		}

		locsInfo := make(map[int]bool)
		for _, locs := range locs {
			locsInfo[locs] = false // invalid now
		}
		s.state.UUIDToLocs[id] = locsInfo

		log.Infof("uuid %v -> locs %v", id, locs)
	}

	return &protos.CreateReply{BlockSize: blockSize}, nil
}

func (s *namenodeServer) Open(ctx context.Context, in *protos.OpenRequest) (*protos.OpenReply, error) {
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
	}()

	log.Infof("namenode server %v open path %v", s.addr, in.Path)

	// check file existence
	info, ok := s.state.FileToInfo[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("path %v not exists", in.Path))
	}

	if utils.IsDir(in.Path) {
		return nil, errors.New(fmt.Sprintf("cannot open dir %v", in.Path))
	}

	// return blocks
	return &protos.OpenReply{BlockSize: blockSize, Blocks: uint64(len(info.Ids))}, nil
}

func (s *namenodeServer) LocsValidityNotify(ctx context.Context, in *protos.LocsValidityNotifyRequest) (*protos.LocsValidityNotifyReply, error) {
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

	locsInfo, ok := s.state.UUIDToLocs[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("uuid %v not exists", id))
	}

	for addr, validity := range in.Validity {
		loc, err := s.isDataNodeExist(addr)
		if err != nil {
			log.Warnf("addr %v not exists", addr)
		} else {
			_, ok := locsInfo[loc]
			if !ok {
				log.Warnf("loc %v not exists", addr)
			} else {
				s.state.UUIDToLocs[id][loc] = validity
			}
		}
	}

	return &protos.LocsValidityNotifyReply{}, nil
}

func (s *namenodeServer) FetchFileInfo(ctx context.Context, in *protos.FetchFileInfoRequest) (*protos.FetchFileInfoReply, error) {
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
	}()

	log.Infof("namenode server %v stat path %v", s.addr, in.Path)

	// check file existence
	info, ok := s.state.FileToInfo[in.Path]
	if !ok {
		return nil, errors.New(fmt.Sprintf("path %v not exists", in.Path))
	}

	var infos []*protos.FileInfo
	if !utils.IsDir(in.Path) { // is file
		infos = append(infos, &protos.FileInfo{
			Name: in.Path,
			Size: info.Size,
		})
	} else { // is directory
		for file, info := range s.state.FileToInfo {
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

func (s *namenodeServer) Rename(ctx context.Context, in *protos.RenameRequest) (*protos.RenameReply, error) {
	if !s.isLeader() {
		return nil, errors.New(fmt.Sprintf("namenode server %v is not leader", s.addr))
	}
	s.mu.Lock()
	defer func() {
		s.syncPropose()
		s.mu.Unlock()
	}()

	log.Infof("namenode server %v trying to rename %v -> %v", s.addr, in.OldPath, in.NewPath)

	// check path existence
	info, ok := s.state.FileToInfo[in.OldPath]
	if !ok {
		return nil, errors.New(fmt.Sprintf("path %v not exists", in.OldPath))
	}

	if !utils.IsDir(in.OldPath) && !utils.IsDir(in.NewPath) {
		delete(s.state.FileToInfo, in.OldPath)
		s.state.FileToInfo[in.NewPath] = info
	} else {
		return nil, errors.New("only support rename from file to file")
	}

	return &protos.RenameReply{}, nil
}

func (s *namenodeServer) IsLeader(ctx context.Context, in *protos.IsLeaderRequest) (*protos.IsLeaderReply, error) {
	return &protos.IsLeaderReply{Res: s.isLeader()}, nil
}
