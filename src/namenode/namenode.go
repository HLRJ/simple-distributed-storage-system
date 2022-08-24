package namenode

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	log "github.com/sirupsen/logrus"
	"simple-distributed-storage-system/src/protos"
	"simple-distributed-storage-system/src/utils"
	"sort"
	"sync"
	"time"
)

// TODO: intro configuration file
const (
	replicaFactor     = 3
	heartbeatDuration = 2
	syncReadDuration  = 2

	blockSize uint64 = 4096
)

type fileInfo struct {
	Ids  []uuid.UUID
	Size uint64
}

type locInfo struct {
	Addr   string
	Blocks uint64
}

type namenodeState struct {
	MaxLoc     int
	LocToInfo  map[int]locInfo
	FileToInfo map[string]fileInfo
	UUIDToLocs map[uuid.UUID]map[int]bool
}

type registrationInfo struct {
	context bool
	addr    string
}

type namenodeServer struct {
	protos.UnimplementedNameNodeServer

	mu        sync.Mutex
	addr      string
	replicaID uint64
	nh        *dragonboat.NodeHost
	state     namenodeState

	registrationInfo registrationInfo
}

func (s *namenodeServer) syncRead(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("namenode server %v stop sync read", s.addr)
			return

		case <-time.After(syncReadDuration * time.Second):
			if s.isLeader() {
				log.Infof("namenode server %v is leader, skip sync read, now state %v", s.addr, s.state)
				break // not return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			result, err := s.nh.SyncRead(ctx, sharedID, []byte{})
			cancel()

			if err == nil {
				s.mu.Lock()
				s.decodeState(result.([]byte))
				s.mu.Unlock()
			} else {
				log.Warn(err)
			}
		}
	}
}

func (s *namenodeServer) syncPropose() {
	cs := s.nh.GetNoOPSession(sharedID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := s.nh.SyncPropose(ctx, cs, s.encodeState())
	cancel()

	if err != nil {
		// TODO: forced synchronization
		log.Warnf("namenode server %v sync propose returned error %v", s.addr, err)
	} else {
		log.Infof("namenode server %v successfully syncing propose", s.addr)
	}
}

func (s *namenodeServer) isLeader() bool {
	id, _, valid, err := s.nh.GetLeaderID(sharedID)
	if err != nil {
		return false
	}
	if id == s.replicaID && valid {
		return true
	}
	return false
}

func (s *namenodeServer) encodeState() []byte {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(s.state)
	if err != nil {
		log.Panic(err)
	}
	log.Infof("namenode server %v encoded state", s.addr)
	return w.Bytes()
}

func (s *namenodeServer) decodeState(data []byte) {
	log.Infof("namenode server %v decode state", s.addr)
	r := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(r)
	var state namenodeState
	err := decoder.Decode(&state)
	if err != nil {
		log.Panic(err)
	}
	s.state = state
}

func (s *namenodeServer) isDataNodeExist(addr string) (int, error) {
	for loc, info := range s.state.LocToInfo {
		if info.Addr == addr {
			return loc, nil
		}
	}
	return 0, errors.New("not found")
}

func (s *namenodeServer) fetchLocs(locs []int, count int) ([]int, error) {
	if len(locs) < count {
		return nil, errors.New("insufficient locs")
	}

	type tmpLocInfo struct {
		loc    int
		blocks uint64
	}

	var tmp []tmpLocInfo
	for _, loc := range locs {
		info, ok := s.state.LocToInfo[loc]
		if ok {
			tmp = append(tmp, tmpLocInfo{
				loc:    loc,
				blocks: info.Blocks,
			})
		} else {
			if !s.registrationInfo.context || loc != s.state.MaxLoc {
				return nil, errors.New("broken invariant")
			}
			tmp = append(tmp, tmpLocInfo{
				loc:    loc,
				blocks: 0,
			})
		}
	}

	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].blocks < tmp[j].blocks
	})

	var res []int
	for i := 0; i < count; i++ {
		res = append(res, tmp[i].loc)
	}
	return res, nil
}

func (s *namenodeServer) fetchAllLocs() []int {
	locs := make([]int, 0)
	for loc, _ := range s.state.LocToInfo {
		locs = append(locs, loc)
	}
	if s.registrationInfo.context {
		// the loc for the latest registered datanode
		locs = append(locs, s.state.MaxLoc)
	}
	return locs
}

func (s *namenodeServer) removeDataNodeServer(loc int, addr string) bool {
	log.Infof("namenode server %v trying to remove datanode server %v with loc %v", s.addr, addr, loc)

	// start data migration
	if s.dataMigration(loc) {
		// delete addr <-> loc
		delete(s.state.LocToInfo, loc)
		log.Infof("namenode server %v successfully removing datanode server %v with loc %v", s.addr, addr, loc)
		return true
	}

	log.Warnf("namenode server %v cannot remove datanode server %v with loc %v", s.addr, addr, loc)
	return false
}

func (s *namenodeServer) dataMigration(loc int) bool {
	log.Infof("namenode server %v start data migration for loc %v", s.addr, loc)
	failed := false

	for id, locsInfo := range s.state.UUIDToLocs {
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
			res, err := s.fetchLocs(candidates, 1)
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}

			// get addr
			toLoc := res[0]
			var toAddr string
			if toLoc == s.state.MaxLoc {
				toAddr = s.registrationInfo.addr
			} else {
				info, ok := s.state.LocToInfo[toLoc]
				if !ok {
					log.Warnf("unable to migrate data for %v", id)
					failed = true
					break
				} else {
					toAddr = info.Addr
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
			var fromAddr string
			info, ok := s.state.LocToInfo[fromLoc]
			if !ok {
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			} else {
				fromAddr = info.Addr
			}

			log.Infof("uuid %v -> copy from %v to %v", id, fromAddr, toAddr)

			// connect to datanode server and read data
			datanode, conn, err := utils.ConnectToTargetDataNode(fromAddr)
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}

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
			conn.Close()

			// connect to datanode server and write data
			datanode, conn, err = utils.ConnectToTargetDataNode(toAddr)
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}

			_, err = datanode.Write(context.Background(), &protos.WriteRequest{Uuid: bin, Data: reply.Data})
			if err != nil {
				log.Warn(err)
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
			}
			conn.Close()

			// modify uuidToDataNodeLocsInfo
			delete(s.state.UUIDToLocs[id], loc)
			s.state.UUIDToLocs[id][toLoc] = true
		}
	}

	return !failed
}

func (s *namenodeServer) heartbeatTicker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("namenode server %v stop heartbeat", s.addr)
			return

		case <-time.After(heartbeatDuration * time.Second):
			if !s.isLeader() {
				break // not return
			}

			s.mu.Lock()
			log.Infof("namenode server %v start heartbeat ticker", s.addr)

			locs := s.fetchAllLocs()
			for _, loc := range locs {
				info, ok := s.state.LocToInfo[loc]
				if ok {
					addr := info.Addr
					datanode, conn, err := utils.ConnectToTargetDataNode(addr)
					if err != nil {
						// delete unreachable datanode server
						log.Warn(err)
						log.Infof("namenode server %v find datanode %v unreachable", s.addr, addr)
						if s.removeDataNodeServer(loc, addr) { // passive remove
							s.syncPropose()
						}
						continue
					}

					reply, err := datanode.HeartBeat(context.Background(), &protos.HeartBeatRequest{})
					if err != nil {
						// delete unreachable datanode server
						log.Warn(err)
						log.Infof("namenode server %v find datanode %v unreachable", s.addr, addr)
						if s.removeDataNodeServer(loc, addr) { // passive remove
							s.syncPropose()
						}
					} else {
						// update block number
						s.state.LocToInfo[loc] = locInfo{
							Addr:   addr,
							Blocks: reply.BlockNumber,
						}
					}
					conn.Close()
				}
			}

			s.mu.Unlock()
		}
	}
}
