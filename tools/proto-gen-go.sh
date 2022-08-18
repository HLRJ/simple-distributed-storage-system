#!/bin/bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
protoc -I ../protos ../protos/namenode.proto --go_out=../protos --go-grpc_out=../protos
