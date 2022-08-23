package datanode

import (
	"context"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"os"
	"simple-distributed-storage-system/src/protos"
)

// Read 读文件
func (s *datanodeServer) Read(ctx context.Context, req *protos.ReadRequest) (*protos.ReadReply, error) {
	id := uuid.New()
	err := id.UnmarshalBinary(req.Uuid)
	if err != nil {
		log.Panic(err)
	}

	filepath := s.localFileSystemRoot() + id.String()
	log.Infof("datanode server %v start to read the file: %v", s.addr, filepath)
	data, err := os.ReadFile(filepath)
	if err != nil {
		log.Panic(err)
	}

	return &protos.ReadReply{Data: data}, nil
}

// Write 写入文件到磁盘
func (s *datanodeServer) Write(ctx context.Context, req *protos.WriteRequest) (*protos.WriteReply, error) {
	id := uuid.New()
	err := id.UnmarshalBinary(req.Uuid)
	if err != nil {
		log.Panic(err)
	}

	data := req.Data
	filepath := s.localFileSystemRoot() + id.String()
	log.Infof("datanode server %v start to write the file: %v", s.addr, filepath)

	file, err := os.Create(filepath)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		log.Panic(err)
	}
	s.blockNumber++
	return &protos.WriteReply{}, nil
}

// HeartBeat 返回心跳包
func (s *datanodeServer) HeartBeat(ctx context.Context, in *protos.HeartBeatRequest) (*protos.HeartBeatReply, error) {
	return &protos.HeartBeatReply{BlockNumber: s.blockNumber}, nil
}

// Remove 删除文件
func (s *datanodeServer) Remove(ctx context.Context, req *protos.RemoveRequest) (*protos.RemoveReply, error) {
	id := uuid.New()
	err := id.UnmarshalBinary(req.Uuid)
	if err != nil {
		log.Panic(err)
	}

	filepath := s.localFileSystemRoot() + id.String()
	log.Infof("datanode server %v start to remove the file: %v", s.addr, filepath)

	err = os.Remove(filepath)
	if err != nil {
		log.Panic(err)
	}
	s.blockNumber--
	return &protos.RemoveReply{}, nil
}
