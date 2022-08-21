package client

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"simple-distributed-storage-system/src/protos"
	"simple-distributed-storage-system/src/utils"
)

func (c *client) Get(remotePath, localPath string) error {
	var data []byte

	blocks, err := c.open(remotePath)
	if err != nil {
		return err
	}

	for i := 0; i < blocks; i++ {
		// get block locs
		reply, err := c.nameNode.FetchBlockAddrs(context.Background(), &protos.FetchBlockAddrsRequest{
			Path:  remotePath,
			Index: uint64(i),
			Type:  protos.FetchBlockAddrsRequestType_OP_GET,
		})
		if err != nil {
			return err
		}

		success := false
		for _, addr := range reply.Addrs {
			// connect to datanode and read data
			datanode, conn, err := utils.ConnectToDataNode(addr)
			if err != nil {
				log.Warn(err)
				continue
			}

			reply, err := datanode.Read(context.Background(), &protos.ReadRequest{
				Uuid: reply.Uuid,
			})

			if err == nil {
				success = true
				data = append(data, reply.Data...)
				conn.Close()
				break
			} else {
				log.Warn(err)
				conn.Close()
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

func (c *client) Put(localPath, remotePath string) error {
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
		reply, err := c.nameNode.FetchBlockAddrs(context.Background(), &protos.FetchBlockAddrsRequest{
			Path:  remotePath,
			Index: uint64(i),
			Type:  protos.FetchBlockAddrsRequestType_OP_PUT,
		})
		if err != nil {
			return err
		}

		validity := make(map[string]bool)

		for _, addr := range reply.Addrs {
			// connect to datanode and write data
			success := true
			datanode, conn, err := utils.ConnectToDataNode(addr)
			if err != nil {
				log.Warn(err)
				success = false
				continue
			}

			_, err = datanode.Write(context.Background(), &protos.WriteRequest{
				Uuid: reply.Uuid,
				Data: data[uint64(i)*c.blockSize : utils.Min(uint64(i+1)*c.blockSize, size)],
			})
			if err != nil {
				log.Warn(err)
				success = false
			}
			conn.Close()

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
		reply, err := c.nameNode.FetchBlockAddrs(context.Background(), &protos.FetchBlockAddrsRequest{
			Path:  remotePath,
			Index: uint64(i),
			Type:  protos.FetchBlockAddrsRequestType_OP_REMOVE,
		})
		if err != nil {
			return err
		}

		validity := make(map[string]bool)

		for _, addr := range reply.Addrs {
			// connect to datanode and remove data
			success := true
			datanode, conn, err := utils.ConnectToDataNode(addr)
			if err != nil {
				log.Warn(err)
				success = false
				continue
			}

			_, err = datanode.Remove(context.Background(), &protos.RemoveRequest{
				Uuid: reply.Uuid,
			})
			if err != nil {
				log.Warn(err)
				success = false
			}
			conn.Close()

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

func (c *client) Stat(remotePath string) (*protos.FileInfo, error) {
	reply, err := c.nameNode.FetchFileInfo(context.Background(), &protos.FetchFileInfoRequest{Path: remotePath})
	if err != nil {
		return nil, err
	}
	return reply.Infos[0], nil
}

func (c *client) Mkdir(remotePath string) error {
	err := c.create(remotePath, 0)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) Rename(remotePathSrc, remotePathDest string) error {
	_, err := c.nameNode.Rename(context.Background(), &protos.RenameRequest{OldPath: remotePathSrc, NewPath: remotePathDest})
	if err != nil {
		return err
	}
	return nil
}

func (c *client) List(remotePath string) ([]*protos.FileInfo, error) {
	reply, err := c.nameNode.FetchFileInfo(context.Background(), &protos.FetchFileInfoRequest{Path: remotePath})
	if err != nil {
		return nil, err
	}
	return reply.Infos, nil
}
