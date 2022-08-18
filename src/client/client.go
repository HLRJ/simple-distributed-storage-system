package client

import (
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io/ioutil"
	"os"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"
	"simple-distributed-storage-system/src/utils"
)

type client struct {
	blockSize uint64

	nameNode protos.NameNodeClient
	conn     *grpc.ClientConn // for close
}

func NewClient() *client {
	// connect to namenode
	conn, err := grpc.Dial(consts.NameNodeServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Panic(err)
	}

	return &client{
		blockSize: 0,
		nameNode:  protos.NewNameNodeClient(conn),
		conn:      conn,
	}
}

func (c *client) CloseClient() {
	err := c.conn.Close()
	if err != nil {
		log.Panic(err)
	}
}

func connectDataNode(addr string) (protos.DataNodeClient, *grpc.ClientConn) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Panic(err)
	}

	return protos.NewDataNodeClient(conn), conn
}

func (c *client) create(remotePath string, size uint64) {
	// create file
	reply, err := c.nameNode.Create(context.Background(), &protos.CreateRequest{
		Path: remotePath,
		Size: size,
	})
	if err != nil {
		log.Panic(err)
	}

	// set block size
	c.blockSize = reply.BlockSize
}

func (c *client) open(remotePath string) int {
	// open file
	reply, err := c.nameNode.Open(context.Background(), &protos.OpenRequest{
		Path: remotePath,
	})
	if err != nil {
		log.Panic(err)
	}

	// set block size
	c.blockSize = reply.BlockSize
	return int(reply.Blocks)
}

func (c *client) Put(localPath string, remotePath string) {
	// read file
	data, err := ioutil.ReadFile(localPath)
	if err != nil {
		log.Panic(err)
	}

	size := uint64(len(data))
	c.create(remotePath, size)

	blocks := utils.CeilDiv(size, c.blockSize)
	for i := 0; i < blocks; i++ {
		// get block locs
		reply, err := c.nameNode.GetBlockAddrs(context.Background(), &protos.GetBlockAddrsRequest{
			Path:  remotePath,
			Index: uint64(i),
		})
		if err != nil {
			log.Panic(err)
		}

		for _, addr := range reply.Addrs {
			// connect to datanode and write data
			datanode, conn := connectDataNode(addr)
			_, err := datanode.Write(context.Background(), &protos.WriteRequest{
				Uuid: reply.Uuid,
				Data: data[uint64(i)*c.blockSize : utils.Min(uint64(i+1)*c.blockSize, size)],
			})
			if err != nil {
				log.Panic(err)
			}
			err = conn.Close()
			if err != nil {
				log.Panic(err)
			}
		}
	}
}

func (c *client) Get(remotePath string, localPath string) {
	var data []byte

	blocks := c.open(remotePath)
	for i := 0; i < blocks; i++ {
		// get block locs
		reply, err := c.nameNode.GetBlockAddrs(context.Background(), &protos.GetBlockAddrsRequest{
			Path:  remotePath,
			Index: uint64(i),
		})
		if err != nil {
			log.Panic(err)
		}

		success := false
		for _, addr := range reply.Addrs {
			// connect to datanode and read data
			datanode, conn := connectDataNode(addr)
			reply, err := datanode.Read(context.Background(), &protos.ReadRequest{
				Uuid: reply.Uuid,
			})

			closeConn := func() {
				err = conn.Close()
				if err != nil {
					log.Panic(err)
				}
			}

			if err == nil {
				success = true
				data = append(data, reply.Data...)
				closeConn()
				break
			} else {
				log.Warn(err)
				closeConn()
				continue
			}
		}

		if !success {
			log.Panic("data corrupted")
		}
	}

	// write to local file
	f, err := os.Create(localPath)
	if err != nil {
		log.Panic(err)
	}
	_, err = f.Write(data)
	if err != nil {
		log.Panic(err)
	}
}
