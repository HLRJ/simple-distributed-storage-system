package namenode

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	log "github.com/sirupsen/logrus"
	"simple-distributed-storage-system/src/protos"
	"simple-distributed-storage-system/src/utils"
	"sync"
	"time"
)

const (
	replicaFactor     = 3
	heartbeatDuration = 5
	syncReadDuration  = 3

	blockSize uint64 = 64
)

type fileInfo struct {
	Ids  []uuid.UUID
	Size uint64
}

type nameNodeState struct {
	DataNodeMaxLoc         int
	DataNodeLocToAddr      map[int]string
	DataNodeAddrToLoc      map[string]int
	FileToInfo             map[string]fileInfo
	UUIDToDataNodeLocsInfo map[uuid.UUID]map[int]bool
}

type registrationInfo struct {
	context bool
	addr    string
}

type nameNodeServer struct {
	protos.UnimplementedNameNodeServer

	mu        sync.Mutex
	addr      string
	replicaID uint64
	nh        *dragonboat.NodeHost
	sm        nameNodeState

	registrationInfo registrationInfo
}

func (s *nameNodeServer) syncRead(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("namenode server %v stop sync read", s.addr)
			return

		case <-time.After(syncReadDuration * time.Second):
			if s.isLeader() {
				log.Infof("namenode server %v is leader, skip sync read, now state %v", s.addr, s.sm)
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

func (s *nameNodeServer) syncPropose() {
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

func (s *nameNodeServer) isLeader() bool {
	id, _, valid, err := s.nh.GetLeaderID(sharedID)
	if err != nil {
		return false
	}
	if id == s.replicaID && valid {
		return true
	}
	return false
}

func (s *nameNodeServer) encodeState() []byte {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(s.sm)
	if err != nil {
		log.Panic(err)
	}
	log.Infof("namenode server %v encoded state %v", s.addr, w.Bytes())
	return w.Bytes()
}

func (s *nameNodeServer) decodeState(state []byte) {
	log.Infof("namenode server %v decode state %v", s.addr, state)
	r := bytes.NewBuffer(state)
	decoder := gob.NewDecoder(r)
	var sm nameNodeState
	err := decoder.Decode(&sm)
	if err != nil {
		log.Panic(err)
	}
	s.sm = sm
}

func (s *nameNodeServer) fetchAllLocs() []int {
	index := 0
	locs := make([]int, len(s.sm.DataNodeLocToAddr))
	for loc := range s.sm.DataNodeLocToAddr {
		locs[index] = loc
		index++
	}
	if s.registrationInfo.context {
		locs = append(locs, s.sm.DataNodeMaxLoc)
	}
	return locs
}

func (s *nameNodeServer) removeDataNodeServer(loc int, addr string) bool {
	log.Infof("namenode server %v trying to remove datanode server %v with loc %v", s.addr, addr, loc)

	// start data migration
	if s.dataMigration(loc) {
		// delete addr <-> loc
		delete(s.sm.DataNodeLocToAddr, loc)
		delete(s.sm.DataNodeAddrToLoc, addr)

		log.Infof("namenode server %v successfully removing datanode server %v with loc %v", s.addr, addr, loc)
		return true
	}

	log.Warnf("namenode server %v cannot remove datanode server %v with loc %v", s.addr, addr, loc)
	return false
}

func (s *nameNodeServer) dataMigration(loc int) bool {
	log.Infof("namenode server %v start data migration for loc %v", s.addr, loc)
	failed := false

	for id, locsInfo := range s.sm.UUIDToDataNodeLocsInfo {
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
			if toLoc == s.sm.DataNodeMaxLoc {
				toAddr = s.registrationInfo.addr
			} else {
				toAddr, ok = s.sm.DataNodeLocToAddr[toLoc]
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
			fromAddr, ok := s.sm.DataNodeLocToAddr[fromLoc]
			if !ok {
				log.Warnf("unable to migrate data for %v", id)
				failed = true
				break
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
			delete(s.sm.UUIDToDataNodeLocsInfo[id], loc)
			s.sm.UUIDToDataNodeLocsInfo[id][toLoc] = true
		}
	}

	return !failed
}

func (s *nameNodeServer) heartbeatTicker(ctx context.Context) {
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
				addr, ok := s.sm.DataNodeLocToAddr[loc]
				if ok {
					datanode, conn, err := utils.ConnectToTargetDataNode(addr)
					if err != nil {
						// delete unreachable datanode server
						log.Warn(err)
						log.Infof("namenode server %v find datanode %v unreachable", s.addr, addr)
						s.removeDataNodeServer(loc, addr) // passive remove
						continue
					}

					_, err = datanode.HeartBeat(context.Background(), &protos.HeartBeatRequest{})
					if err != nil {
						// delete unreachable datanode server
						log.Warn(err)
						log.Infof("namenode server %v find datanode %v unreachable", s.addr, addr)
						s.removeDataNodeServer(loc, addr) // passive remove
					}
					conn.Close()
				}
			}

			s.mu.Unlock()
		}
	}
}
