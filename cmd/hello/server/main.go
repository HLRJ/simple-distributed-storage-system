package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"net"
	"simple-distributed-storage-system/src/protos"
)

type helloServer struct {
	protos.UnimplementedGreeterServer
}

func (s *helloServer) SayHello(ctx context.Context, req *protos.HelloRequest) (res *protos.HelloReply, err error) {
	return &protos.HelloReply{Message: fmt.Sprintf("hello %v", req.GetName())}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":8888")
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	protos.RegisterGreeterServer(server, &helloServer{})
	err = server.Serve(listener)
	if err != nil {
		panic(err)
	}
}
