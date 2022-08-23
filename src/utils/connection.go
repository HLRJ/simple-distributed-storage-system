package utils

import (
	"context"
	"errors"
	"fmt"
	zipkingrpc "github.com/openzipkin/zipkin-go/middleware/grpc"
	"github.com/openzipkin/zipkin-go/reporter"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"
	"sync/atomic"
	"time"
)

var opts uint64 = 0

type ConnHandler struct {
	conn     *grpc.ClientConn
	reporter reporter.Reporter
}

func (h *ConnHandler) Close() {
	h.conn.Close()
	h.reporter.Close()
}

func ConnectToTargetDataNode(addr string) (protos.DataNodeClient, *ConnHandler, error) {
	// zipkin
	no := atomic.LoadUint64(&opts)
	tracer, r, err := NewZipkinTracer(ZIPKIN_HTTP_ENDPOINT, fmt.Sprintf("DataNode-Client-%s-%v", addr, no), addr)
	atomic.AddUint64(&opts, 1)
	if err != nil {
		r.Close()
		return nil, nil, err
	}
	conn, err := grpc.Dial(addr, grpc.WithStatsHandler(zipkingrpc.NewClientHandler(tracer)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		r.Close()
		return nil, nil, err
	}
	return protos.NewDataNodeClient(conn), &ConnHandler{
		conn:     conn,
		reporter: r,
	}, nil
}

func ConnectToTargetNameNode(addr string, readonly bool) (protos.NameNodeClient, *ConnHandler, error) {
	// zipkin
	no := atomic.LoadUint64(&opts)
	tracer, r, err := NewZipkinTracer(ZIPKIN_HTTP_ENDPOINT, fmt.Sprintf("NameNode-Client-%s-%v", addr, no), addr)
	atomic.AddUint64(&opts, 1)
	if err != nil {
		r.Close()
		return nil, nil, err
	}
	conn, err := grpc.Dial(addr, grpc.WithStatsHandler(zipkingrpc.NewClientHandler(tracer)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		r.Close()
		return nil, nil, err
	}
	namenode := protos.NewNameNodeClient(conn)
	reply, err := namenode.IsLeader(context.Background(), &protos.IsLeaderRequest{})
	if err != nil {
		// unreachable
		r.Close()
		return nil, nil, err
	} else {
		if !reply.Res && !readonly {
			// must connect to leader
			r.Close()
			return nil, nil, errors.New(fmt.Sprintf("namenode server %v is not leader", addr))
		}
	}
	return namenode, &ConnHandler{
		conn:     conn,
		reporter: r,
	}, nil
}

func ConnectToNameNode(readonly bool) (protos.NameNodeClient, *ConnHandler, error) {
	rounds := 0
	for {
		rounds++
		if rounds >= 8 {
			break
		}

		for _, addr := range consts.NameNodeServerAddrs {
			namenode, conn, err := ConnectToTargetNameNode(addr, readonly)
			if err != nil {
				log.Warn(err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			log.Infof("connect to namenode server %v", addr)
			return namenode, conn, nil
		}
	}

	return nil, nil, errors.New("no available namenode server")
}
