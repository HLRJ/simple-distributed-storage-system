#!/bin/bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
protoc -I ../src/protos ../src/protos/namenode.proto --go_out=../src/protos --go-grpc_out=../src/protos
protoc -I ../src/protos ../src/protos/datanode.proto --go_out=../src/protos --go-grpc_out=../src/protos