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
	"time"

	log "github.com/sirupsen/logrus"
)

type dataNodeServer struct {
	protos.UnimplementedDataNodeServer
	addr      string
	blockSize uint64
}

func (s *dataNodeServer) localFileSystemRoot() string {
	return "/tmp/gfs/chunks/" + s.addr + "/"
}

// Read 读文件
func (s *dataNodeServer) Read(ctx context.Context, req *protos.ReadRequest) (*protos.ReadReply, error) {
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
func (s *dataNodeServer) Write(ctx context.Context, req *protos.WriteRequest) (*protos.WriteReply, error) {
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
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Panic(err)
		}
	}(file)

	_, err = file.Write(data)
	if err != nil {
		log.Panic(err)
	}
	return &protos.WriteReply{}, nil
}

// HeartBeat 返回心跳包
func (s *dataNodeServer) HeartBeat(ctx context.Context, in *protos.HeartBeatRequest) (*protos.HeartBeatReply, error) {
	return &protos.HeartBeatReply{}, nil
}

// Remove 删除文件
func (s *dataNodeServer) Remove(ctx context.Context, req *protos.RemoveRequest) (*protos.RemoveReply, error) {
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
	return &protos.RemoveReply{}, nil
}

func NewDataNodeServer(addr string) *dataNodeServer {
	return &dataNodeServer{
		addr: addr,
	}
}

func (s *dataNodeServer) Setup(ctx context.Context) {
	// create local fs
	err := os.MkdirAll(s.localFileSystemRoot(), os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	// setup datanode server
	log.Infof("starting datanode server %v", s.addr)
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
retry:
	reply, err := client.RegisterDataNode(context.Background(), &protos.RegisterDataNodeRequest{Address: s.addr})
	if err != nil {
		log.Warn(err)
		time.Sleep(time.Second)
		// TODO: max retries
		goto retry
	}

	// set block size
	s.blockSize = reply.BlockSize

	// blocked here
	select {
	case <-ctx.Done():
		server.Stop()
		log.Infof("datanode server %v quitting", s.addr)
		return
	}
}
