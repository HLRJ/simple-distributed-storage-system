package client

import (
	"context"
	"google.golang.org/grpc"
	"simple-distributed-storage-system/src/protos"
)

type client struct {
	blockSize uint64
	nameNode  protos.NameNodeClient
	conn      *grpc.ClientConn // for close
}

func (c *client) create(remotePath string, size uint64) error {
	// create file
	reply, err := c.nameNode.Create(context.Background(), &protos.CreateRequest{
		Path: remotePath,
		Size: size,
	})
	if err != nil {
		return err
	}

	// set block size
	c.blockSize = reply.BlockSize

	return nil
}

func (c *client) open(remotePath string) (int, error) {
	// open file
	reply, err := c.nameNode.Open(context.Background(), &protos.OpenRequest{
		Path: remotePath,
	})
	if err != nil {
		return 0, err
	}

	// set block size
	c.blockSize = reply.BlockSize
	return int(reply.Blocks), nil
}
