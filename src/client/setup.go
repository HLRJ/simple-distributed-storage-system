package client

import (
	log "github.com/sirupsen/logrus"
	"simple-distributed-storage-system/src/utils"
)

func NewClient() *client {
	// connect to namenode
	nameNode, conn, err := utils.ConnectToNameNode(false)
	if err != nil {
		log.Panic(err)
	}
	return &client{
		blockSize: 0,
		nameNode:  nameNode,
		conn:      conn,
	}
}

func NewReadonlyClient() *client {
	// connect to namenode
	nameNode, conn, err := utils.ConnectToNameNode(true)
	if err != nil {
		log.Panic(err)
	}
	return &client{
		blockSize: 0,
		nameNode:  nameNode,
		conn:      conn,
	}
}

func (c *client) CloseClient() {
	c.conn.Close()
}
