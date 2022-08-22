package datanode

import (
	"context"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"simple-distributed-storage-system/src/protos"
)

// Read 读文件
func (s *dataNodeServer) Read(ctx context.Context, req *protos.ReadRequest) (*protos.ReadReply, error) {
	id := uuid.New()
	err := id.UnmarshalBinary(req.Uuid)
	if err != nil {
		log.Panic(err)
	}
	data, b, err := s.lstmtree.Get(id.NodeID())
	if err != nil {
		return nil, err
	}
	//filepath := s.localFileSystemRoot() + id.String()
	log.Infof("datanode server %v start to read the file: %v", s.addr, s.localFileSystemRoot()+id.String())
	//data, err := os.ReadFile(filepath)
	//if err != nil {
	//	log.Panic(err)
	//}
	if b {
		return &protos.ReadReply{Data: data}, nil
	}
	return nil, nil

}

// Write 写入文件到磁盘
func (s *dataNodeServer) Write(ctx context.Context, req *protos.WriteRequest) (*protos.WriteReply, error) {
	id := uuid.New()
	err := id.UnmarshalBinary(req.Uuid)
	if err != nil {
		log.Panic(err)
	}

	data := req.Data
	//filepath := s.localFileSystemRoot() + id.String()
	log.Infof("datanode server %v start to write the file %v", s.addr, s.localFileSystemRoot()+id.String())
	//
	//file, err := os.Create(filepath)
	//if err != nil {
	//	log.Panic(err)
	//}
	//defer file.Close()
	//
	//_, err = file.Write(data)
	//if err != nil {
	//	log.Panic(err)
	//}
	err = s.lstmtree.Put(id.NodeID(), data)
	if err != nil {
		return nil, err
	}
	s.blockNumber++
	return &protos.WriteReply{}, nil
}

// HeartBeat 返回心跳包
func (s *dataNodeServer) HeartBeat(ctx context.Context, in *protos.HeartBeatRequest) (*protos.HeartBeatReply, error) {
	return &protos.HeartBeatReply{BlockNumber: s.blockNumber}, nil
}

// Remove 删除文件
func (s *dataNodeServer) Remove(ctx context.Context, req *protos.RemoveRequest) (*protos.RemoveReply, error) {
	id := uuid.New()
	err := id.UnmarshalBinary(req.Uuid)
	if err != nil {
		log.Panic(err)
	}

	//filepath := s.localFileSystemRoot() + id.String()
	//log.Infof("datanode server %v start to remove the file: %v", s.addr, filepath)
	//
	//err = os.Remove(filepath)
	//if err != nil {
	//	log.Panic(err)
	//}
	err = s.lstmtree.Delete(id.NodeID())
	if err != nil {
		return nil, err
	}
	log.Infof("datanode server %v start to remove the file %v", s.addr, s.localFileSystemRoot()+id.String())
	s.blockNumber--
	return &protos.RemoveReply{}, nil
}
