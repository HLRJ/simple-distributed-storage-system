package datanode

import (
	"context"
	"os"
	"simple-distributed-storage-system/src/protos"

	log "github.com/sirupsen/logrus"
)

const LocalFileSystemRoot string = "/tmp/gfs/chunks/"

type DataServer struct {
	protos.UnimplementedDataNodeServer
  addr      string
  blockSize uint64
}

// 读文件
func (c *DataServer) Read(ctx context.Context, req *protos.ReadRequest) (*protos.ReadReply, error) {
	uuid := string(req.GetUuid())
	log.Infof("start to read the file: %v", uuid)
	filepath := LocalFileSystemRoot + uuid

	fileStream, errStream := os.ReadFile(filepath)
	if errStream != nil {
		log.Panic("failed to open a file: %v", errStream)
		return &protos.ReadReply{Data: []byte("failed to read a fileStream")}, errStream
	}
	log.Infof("successfully read the file: %v", uuid)
	return &protos.ReadReply{Data: fileStream}, nil
}

// 写入文件到磁盘
func (c *DataServer) Write(ctx context.Context, req *protos.WriteRequest) (*protos.WriteReply, error) {
	uuid := string(req.GetUuid())
	data := req.Data
	log.Infof("start to write the file:%v", uuid)
	filepath := LocalFileSystemRoot + uuid

	fileStream, errStream := os.Create(filepath)
	if errStream != nil {
		log.Panic("failed to create a file:%v", errStream)
	}
	defer fileStream.Close()

	_, errWrite := fileStream.Write(data)
	if errWrite != nil {
		log.Panic("failed to write the file:%v", uuid)
	}
	log.Infof("successfully write the file: %v", uuid)
	return &protos.WriteReply{}, nil
}

// 返回心跳包
func (c *DataServer) HeartBeat(ctx context.Context, in *protos.HeartBeatRequest) (*protos.HeartBeatReply, error) {
	return &protos.HeartBeatReply{}, nil
}

// 删除文件
func (c *DataServer) Remove(ctx context.Context, req *protos.RemoveRequest) (*protos.RemoveReply, error) {
	uuid := string(req.GetUuid())
	log.Infof("start to remove the file:%v", uuid)
	filepath := LocalFileSystemRoot + uuid

	errRemove := os.Remove(filepath)
	if errRemove != nil {
		log.Panic("failed to remove the file:%v", uuid)
	} else {
		log.Infof("successfully remove the file:%v", uuid)
	}
	return &protos.RemoveReply{}, nil
}

func NewDataNodeServer(addr string) *dataNodeServer {
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