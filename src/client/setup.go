package client

import (
	log "github.com/sirupsen/logrus"
	"simple-distributed-storage-system/src/utils"
)

func NewClient(readonly bool) *client {
	// connect to namenode
	namenode, conn, err := utils.ConnectToNameNode(readonly)
	if err != nil {
		log.Panic(err)
	}
	return &client{
		blockSize: 0,
		readonly:  readonly,
		namenode:  namenode,
		conn:      conn,
	}
}

func (c *client) CloseClient() {
	c.conn.Close()
}
