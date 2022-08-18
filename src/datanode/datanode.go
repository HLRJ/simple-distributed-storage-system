package datanode

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"os"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"
)

type dataNodeServer struct {
	protos.DataNodeServer
	blockSize uint64
	kv        *leveldb.DB
	address   string
}

func (s *dataNodeServer) WriteData(ctx context.Context, in *protos.WriteDataRequest) (*protos.WriteDataReply, error) {
	err := s.kv.Put(in.Uuid, in.Data, nil)
	if err != nil {
		return nil, err
	}
	return &protos.WriteDataReply{}, nil
}

func NewDataNodeServer(address string) *dataNodeServer {
	db, err := leveldb.OpenFile(os.TempDir()+"/"+address, nil)
	if err != nil {
		log.Panic(err)
	}
	return &dataNodeServer{
		address: address,
		kv:      db,
	}
}

func (s *dataNodeServer) Setup() {
	// setup datanode server
	log.Infof("starting datanode server at %v", s.address)
	listener, err := net.Listen("tcp", s.address)
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
	reply, err := client.RegisterDataNode(context.Background(), &protos.RegisterDataNodeRequest{Address: s.address})
	if err != nil {
		log.Panic(err)
	}

	// set block size
	s.blockSize = reply.BlockSize

	// blocked here
	select {}
}
