package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"simple-distributed-storage-system/src/protos"
)

func main() {
	conn, err := grpc.Dial("localhost:8888", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := protos.NewGreeterClient(conn)
	res, err := client.SayHello(context.Background(), &protos.HelloRequest{Name: "vgalaxy"})
	if err != nil {
		panic(err)
	}
	fmt.Println(res.Message)
}
