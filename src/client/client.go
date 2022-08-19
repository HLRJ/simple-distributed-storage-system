package client

import (
	"context"
	"errors"
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
	nameNode  protos.NameNodeClient
	conn      *grpc.ClientConn // for close
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

func (c *client) CloseClient() error {
	return c.conn.Close()
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

func (c *client) Get(remotePath string, localPath string) error {
	var data []byte

	blocks, err := c.open(remotePath)
	if err != nil {
		return err
	}

	for i := 0; i < blocks; i++ {
		// get block locs
		reply, err := c.nameNode.GetBlockAddrs(context.Background(), &protos.GetBlockAddrsRequest{
			Path:   remotePath,
			Index:  uint64(i),
			OpType: protos.GetBlockAddrsRequestOpType_OP_GET,
		})
		if err != nil {
			return err
		}

		success := false
		for _, addr := range reply.Addrs {
			// connect to datanode and read data
			datanode, conn := utils.ConnectToDataNode(addr)
			reply, err := datanode.Read(context.Background(), &protos.ReadRequest{
				Uuid: reply.Uuid,
			})

			if err == nil {
				success = true
				data = append(data, reply.Data...)
				err = conn.Close()
				if err != nil {
					return err
				}
				break
			} else {
				log.Warn(err)
				err = conn.Close()
				if err != nil {
					return err
				}
				continue
			}
		}

		if !success {
			return errors.New("data corrupted")
		}
	}

	// write to local file
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) Put(localPath string, remotePath string) error {
	// read file
	data, err := ioutil.ReadFile(localPath)
	if err != nil {
		return err
	}

	size := uint64(len(data))
	err = c.create(remotePath, size)
	if err != nil {
		return err
	}

	blocks := utils.CeilDiv(size, c.blockSize)
	for i := 0; i < blocks; i++ {
		// get block locs
		reply, err := c.nameNode.GetBlockAddrs(context.Background(), &protos.GetBlockAddrsRequest{
			Path:   remotePath,
			Index:  uint64(i),
			OpType: protos.GetBlockAddrsRequestOpType_OP_PUT,
		})
		if err != nil {
			return err
		}

		validity := make(map[string]bool)

		for _, addr := range reply.Addrs {
			// connect to datanode and write data
			success := true
			datanode, conn := utils.ConnectToDataNode(addr)

			_, err := datanode.Write(context.Background(), &protos.WriteRequest{
				Uuid: reply.Uuid,
				Data: data[uint64(i)*c.blockSize : utils.Min(uint64(i+1)*c.blockSize, size)],
			})
			if err != nil {
				log.Warn(err)
				success = false
			}
			err = conn.Close()
			if err != nil {
				return err
			}

			if success {
				validity[addr] = true
			}
		}

		// notify validity
		_, err = c.nameNode.LocsValidityNotify(context.Background(), &protos.LocsValidityNotifyRequest{
			Uuid:     reply.Uuid,
			Validity: validity,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *client) Remove(remotePath string) error {
	blocks, err := c.open(remotePath)
	if err != nil {
		return err
	}

	for i := 0; i < blocks; i++ {
		// get block locs
		reply, err := c.nameNode.GetBlockAddrs(context.Background(), &protos.GetBlockAddrsRequest{
			Path:   remotePath,
			Index:  uint64(i),
			OpType: protos.GetBlockAddrsRequestOpType_OP_REMOVE,
		})
		if err != nil {
			return err
		}

		validity := make(map[string]bool)

		for _, addr := range reply.Addrs {
			// connect to datanode and remove data
			success := true
			datanode, conn := utils.ConnectToDataNode(addr)

			_, err := datanode.Remove(context.Background(), &protos.RemoveRequest{
				Uuid: reply.Uuid,
			})
			if err != nil {
				log.Warn(err)
				success = false
			}
			err = conn.Close()
			if err != nil {
				return err
			}

			if success {
				validity[addr] = false
			}
		}

		// notify validity
		_, err = c.nameNode.LocsValidityNotify(context.Background(), &protos.LocsValidityNotifyRequest{
			Uuid:     reply.Uuid,
			Validity: validity,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
