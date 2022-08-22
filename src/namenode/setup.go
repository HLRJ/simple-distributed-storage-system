package namenode

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	zipkingrpc "github.com/openzipkin/zipkin-go/middleware/grpc"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"path/filepath"
	"simple-distributed-storage-system/src/consts"
	"simple-distributed-storage-system/src/protos"
	"simple-distributed-storage-system/src/utils"
)

const (
	sharedID uint64 = 128
)

func NewNameNodeServer(addr string, replicaID uint64) *nameNodeServer {
	res := &nameNodeServer{
		addr:      addr,
		replicaID: replicaID,
		sm: nameNodeState{
			DataNodeLocToAddr:      make(map[int]string),
			DataNodeAddrToLoc:      make(map[string]int),
			FileToInfo:             make(map[string]fileInfo),
			UUIDToDataNodeLocsInfo: make(map[uuid.UUID]map[int]bool),
		},
	}
	// setup root path
	res.sm.FileToInfo["/"] = fileInfo{
		Ids:  []uuid.UUID{},
		Size: 0,
	}
	return res
}

func (s *nameNodeServer) Setup(ctx context.Context) {
	// setup cluster
	s.setupCluster()
	//zipkin
	tracer, r, err := utils.NewZipkinTracer(utils.ZIPKIN_HTTP_ENDPOINT, "nameNodeServer", s.addr)
	defer r.Close()
	if err != nil {
		log.Println(err)
		return
	}
	// setup namenode server
	log.Infof("starting namenode server at %v", s.addr)
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Panic(err)
	}
	server := grpc.NewServer(grpc.StatsHandler(zipkingrpc.NewServerHandler(tracer)))
	protos.RegisterNameNodeServer(server, s)

	go func() {
		err = server.Serve(listener)
		if err != nil {
			log.Panic(err)
		}
	}()

	// start heartbeat ticker
	go s.heartbeatTicker(ctx)

	// start sync read
	go s.syncRead(ctx)

	// blocked here
	select {
	case <-ctx.Done():
		// stop server
		server.Stop()
		// close cluster
		s.nh.Close()
		log.Infof("namenode server %v quitting", s.addr)
		return
	}
}

func (s *nameNodeServer) setupCluster() {
	initialMembers := make(map[uint64]string)
	for idx, v := range consts.NameNodeServerRaftAddrs {
		// key is the ReplicaID, ReplicaID is not allowed to be 0
		// value is the raft address
		initialMembers[uint64(idx+1)] = v
	}

	// change the log verbosity
	logger.GetLogger("raft").SetLevel(logger.CRITICAL)
	logger.GetLogger("rsm").SetLevel(logger.CRITICAL)
	logger.GetLogger("transport").SetLevel(logger.CRITICAL)
	logger.GetLogger("grpc").SetLevel(logger.CRITICAL)

	// config for raft node
	rc := config.Config{
		// ShardID and ReplicaID of the raft node
		ReplicaID: s.replicaID,
		ShardID:   sharedID,
		// In this example, we assume the end-to-end round trip time (RTT) between
		// NodeHost instances (on different machines, VMs or containers) are 200
		// millisecond, it is set in the RTTMillisecond field of the
		// config.NodeHostConfig instance below.
		// ElectionRTT is set to 10 in this example, it determines that the node
		// should start an election if there is no heartbeat from the leader for
		// 10 * RTT time intervals.
		ElectionRTT: 10,
		// HeartbeatRTT is set to 1 in this example, it determines that when the
		// node is a leader, it should broadcast heartbeat messages to its followers
		// every such 1 * RTT time interval.
		HeartbeatRTT: 1,
		CheckQuorum:  true,
		// SnapshotEntries determines how often should we take a snapshot of the
		// replicated state machine, it is set to 10 her which means a snapshot
		// will be captured for every 10 applied proposals (writes).
		// In your real world application, it should be set to much higher values
		// You need to determine a suitable value based on how much space you are
		// willing use on Raft Logs, how fast can you capture a snapshot of your
		// replicated state machine, how often such snapshot is going to be used
		// etc.
		SnapshotEntries: 100,
		// Once a snapshot is captured and saved, how many Raft entries already
		// covered by the new snapshot should be kept. This is useful when some
		// followers are just a little bit left behind, with such overhead Raft
		// entries, the leaders can send them regular entries rather than the full
		// snapshot image.
		CompactionOverhead: 5,
	}

	datadir := filepath.Join(consts.RaftPersistenceDataDir, fmt.Sprintf("namenode%v", s.replicaID))
	// config for the nodehost
	// See GoDoc for all available options
	// by default, insecure transport is used, you can choose to use Mutual TLS
	// Authentication to authenticate both servers and clients. To use Mutual
	// TLS Authentication, set the MutualTLS field in NodeHostConfig to true, set
	// the CAFile, CertFile and KeyFile fields to point to the path of your CA
	// file, certificate and key files.
	nhc := config.NodeHostConfig{
		// WALDir is the directory to store the WAL of all Raft Logs. It is
		// recommended to use Enterprise SSDs with good fsync() performance
		// to get the best performance. A few SSDs we tested or known to work very
		// well
		// Recommended SATA SSDs -
		// Intel S3700, Intel S3710, Micron 500DC
		// Other SATA enterprise class SSDs with power loss protection
		// Recommended NVME SSDs -
		// Most enterprise NVME currently available on the market.
		// SSD to avoid -
		// Consumer class SSDs, no matter whether they are SATA or NVME based, as
		// they usually have very poor fsync() performance.
		//
		// You can use the pg_test_fsync tool shipped with PostgreSQL to test the
		// fsync performance of your WAL disk. It is recommended to use SSDs with
		// fsync latency of well below 1 millisecond.
		//
		// Note that this is only for storing the WAL of Raft Logs, it is size is
		// usually pretty small, 64GB per NodeHost is usually more than enough.
		//
		// If you just have one disk in your system, just set WALDir and NodeHostDir
		// to the same location.
		WALDir: datadir,
		// NodeHostDir is where everything else is stored.
		NodeHostDir: datadir,
		// RTTMillisecond is the average round trip time between NodeHosts (usually
		// on two machines/vms), it is in millisecond. Such RTT includes the
		// processing delays caused by NodeHosts, not just the network delay between
		// two NodeHost instances.
		RTTMillisecond: 200,
		// RaftAddress is used to identify the NodeHost instance
		RaftAddress: initialMembers[s.replicaID],
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		log.Panic(err)
	}

	if err := nh.StartReplica(initialMembers, false, NewStateMachine, rc); err != nil {
		log.Panicf("namenode server %v failed to add cluster, %v", s.addr, err)
	}

	s.nh = nh
}
