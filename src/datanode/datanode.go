package datanode

import (
	"context"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"os"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"

	log "github.com/sirupsen/logrus"
)

const LocalFileSystemRoot string = "/tmp/gfs/chunks/"

type dataNodeServer struct {
	protos.UnimplementedDataNodeServer
	addr      string
	blockSize uint64
}

// 读文件
func (c *dataNodeServer) Read(ctx context.Context, req *protos.ReadRequest) (*protos.ReadReply, error) {
	id := uuid.New()
	err := id.UnmarshalBinary(req.Uuid)
	if err != nil {
		log.Panic(err)
	}

	log.Infof("start to read the file: %v", id.String())
	filepath := LocalFileSystemRoot + id.String()

	data, err := os.ReadFile(filepath)
	if err != nil {
		log.Panicf("failed to open a file: %v", err)
		return nil, err
	}
	log.Infof("successfully read the file: %v", id.String())
	return &protos.ReadReply{Data: data}, nil
}

// 写入文件到磁盘
func (c *dataNodeServer) Write(ctx context.Context, req *protos.WriteRequest) (*protos.WriteReply, error) {
	id := uuid.New()
	err := id.UnmarshalBinary(req.Uuid)
	if err != nil {
		log.Panic(err)
	}

	data := req.Data
	log.Infof("start to write the file: %v", id.String())
	filepath := LocalFileSystemRoot + id.String()

	file, err := os.Create(filepath)
	if err != nil {
		log.Panicf("failed to create a file: %v", err)
	}
	defer file.Close()

	_, errWrite := file.Write(data)
	if errWrite != nil {
		log.Panicf("failed to write the file: %v", id.String())
	}
	log.Infof("successfully write the file: %v", id.String())
	return &protos.WriteReply{}, nil
}

// 返回心跳包
func (c *dataNodeServer) HeartBeat(ctx context.Context, in *protos.HeartBeatRequest) (*protos.HeartBeatReply, error) {
	return &protos.HeartBeatReply{}, nil
}

// 删除文件
func (c *dataNodeServer) Remove(ctx context.Context, req *protos.RemoveRequest) (*protos.RemoveReply, error) {
	id := uuid.New()
	err := id.UnmarshalBinary(req.Uuid)
	if err != nil {
		log.Panic(err)
	}

	log.Infof("start to remove the file: %v", id.String())
	filepath := LocalFileSystemRoot + id.String()

	errRemove := os.Remove(filepath)
	if errRemove != nil {
		log.Panicf("failed to remove the file: %v", id.String())
	} else {
		log.Infof("successfully remove the file: %v", id.String())
	}
	return &protos.RemoveReply{}, nil
}

func NewDataNodeServer(addr string) *dataNodeServer {
	err := os.MkdirAll(LocalFileSystemRoot, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	return &dataNodeServer{
		addr: addr,
	}
}

func (s *dataNodeServer) Setup() {
	// setup datanode server
	log.Infof("starting datanode server at %v", s.addr)
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Panic(err)
	}
	server := grpc.NewServer()
	protos.RegisterDataNodeServer(server, s)

	go func() {
		err = server.Serve(listener)
		if err != nil {
			log.Panic(err)
		}
	}()

	// connect to namenode
	conn, err := grpc.Dial(consts.NameNodeServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Panic(err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Panic(err)
		}
	}(conn)

	client := protos.NewNameNodeClient(conn)
	reply, err := client.RegisterDataNode(context.Background(), &protos.RegisterDataNodeRequest{Address: s.addr})
	if err != nil {
		log.Panic(err)
	}

	// set block size
	s.blockSize = reply.BlockSize

	// blocked here
	select {}
}
