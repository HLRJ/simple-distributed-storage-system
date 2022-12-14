package namenode

import (
	"bytes"
	"encoding/gob"
	"github.com/google/uuid"
	sm "github.com/lni/dragonboat/v4/statemachine"
	log "github.com/sirupsen/logrus"
	"io"
)

type StateMachine struct {
	ShardID   uint64
	ReplicaID uint64
	State     namenodeState
}

func (s *StateMachine) Update(entry sm.Entry) (sm.Result, error) {
	r := bytes.NewBuffer(entry.Cmd)
	decoder := gob.NewDecoder(r)
	var state namenodeState
	err := decoder.Decode(&state)
	if err != nil {
		log.Panic(err)
	}
	s.State = state
	log.Infof("replica %v update state machine %v", s.ReplicaID, s.State)
	return sm.Result{Value: uint64(len(entry.Cmd))}, nil
}

func (s *StateMachine) Lookup(i interface{}) (interface{}, error) {
	log.Infof("replica %v lookup state machine %v", s.ReplicaID, s.State)
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(s.State)
	if err != nil {
		log.Panic(err)
	}
	return w.Bytes(), nil
}

func (s *StateMachine) SaveSnapshot(writer io.Writer, collection sm.ISnapshotFileCollection, i <-chan struct{}) error {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(s.State)
	if err != nil {
		log.Panic(err)
	}
	_, err = writer.Write(w.Bytes())
	return err
}

func (s *StateMachine) RecoverFromSnapshot(reader io.Reader, files []sm.SnapshotFile, i <-chan struct{}) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Panic(err)
	}
	r := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(r)
	var state namenodeState
	err = decoder.Decode(&state)
	if err != nil {
		log.Panic(err)
	}
	s.State = state
	return nil
}

func (s *StateMachine) Close() error {
	return nil
}

func NewStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	return &StateMachine{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State: namenodeState{
			LocToInfo:  make(map[int]locInfo),
			FileToInfo: make(map[string]fileInfo),
			UUIDToLocs: make(map[uuid.UUID]map[int]bool),
		},
	}
}
