package client

import (
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"simple-distributed-storage-system/src/protos"
	"simple-distributed-storage-system/src/utils"
)

type client struct {
	blockSize uint64
	readonly  bool
	namenode  protos.NameNodeClient
	conn      *grpc.ClientConn // for close
}

func (c *client) create(remotePath string, size uint64) error {
	// create file
	reply, err := c.namenode.Create(context.Background(), &protos.CreateRequest{
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
	reply, err := c.namenode.Open(context.Background(), &protos.OpenRequest{
		Path: remotePath,
	})
	if err != nil {
		return 0, err
	}

	// set block size
	c.blockSize = reply.BlockSize
	return int(reply.Blocks), nil
}

func (c *client) testConnection() {
	reply, err := c.namenode.IsLeader(context.Background(), &protos.IsLeaderRequest{})
	reconnect := false
	if err != nil {
		// unreachable
		log.Warn("namenode server unreachable")
		c.conn.Close()
		reconnect = true
	} else {
		if !reply.Res && !c.readonly {
			// must connect to leader
			log.Warn("namenode server is not leader")
			c.conn.Close()
			reconnect = true
		}
	}
	if reconnect {
		namenode, conn, err := utils.ConnectToNameNode(c.readonly)
		if err != nil {
			log.Panic(err)
		}
		c.namenode = namenode
		c.conn = conn
	}
}
