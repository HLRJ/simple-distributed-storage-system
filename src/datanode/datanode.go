package datanode

import (
	"context"
	"log"
	"os"
	"simple-distributed-storage-system/src/protos"
)

const LocalFileSystemRoot string = "/tmp/gfs/chunks/"

type DataServer struct {
	protos.UnimplementedDataserverServer
}

func (c *DataServer) GetUuidPath(ctx context.Context, req *protos.UuidRequest) (*protos.UuidResponse, error) {
	FilePath := LocalFileSystemRoot + string(req.GetChunkuuid())
	return &protos.UuidResponse{FilePath: FilePath}, nil
}

func (c *DataServer) WriteChunk(ctx context.Context, req *protos.WriteChunkRequest) (*protos.WriteChunkResponse, error) {
	filepath, errUUID := c.GetUuidPath(context.Background(), req.UuidRequest)
	if errUUID != nil {
		log.Fatalf("failed to get uuidPath: %v", errUUID)
		return &protos.WriteChunkResponse{Message: "failed to get uuidPath"}, errUUID
	}

	fileStream, errStream := os.Create(filepath.GetFilePath())

	if errStream != nil {
		log.Fatalf("failed to create a fileStream: %v", errStream)
		return &protos.WriteChunkResponse{Message: "ffailed to create a fileStream"}, errStream
	}
	defer fileStream.Close()
	_, errWrite := fileStream.Write(req.GetChunk())

	if errWrite != nil {
		log.Fatalf("failed to write fileStream to memory:%v", errWrite)
		return &protos.WriteChunkResponse{Message: "failed to write fileStream to memory"}, errWrite
	}
	return &protos.WriteChunkResponse{Message: "success to writeChunk"}, nil

}

func (c *DataServer) ReadChunk(ctx context.Context, req *protos.ReadChunkRequest) (*protos.ReadChunkResponse, error) {
	filepath, errUUID := c.GetUuidPath(context.Background(), req.UuidRequest)
	if errUUID != nil {
		log.Fatalf("failed to get uuidPath: %v", errUUID)
		return &protos.ReadChunkResponse{Chunk: []byte("failed to get uuidPath")}, errUUID
	}
	fileStream, errStream := os.ReadFile(filepath.GetFilePath())
	if errStream != nil {
		log.Fatalf("failed to create a fileStream: %v", errStream)
		return &protos.ReadChunkResponse{Chunk: []byte("failed to read a fileStream")}, errStream
	}

	return &protos.ReadChunkResponse{Chunk: fileStream}, nil
}

//将DataServer结构体供外部调用
func MakeServer() *DataServer {
	Server := &DataServer{}
	return Server
}
