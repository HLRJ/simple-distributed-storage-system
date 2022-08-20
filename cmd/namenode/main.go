package main

import (
	"context"
	"flag"
	"simple-distributed-storage-system/src/namenode"
)

var (
	addr      = flag.String("addr", "localhost:8000", "Node host address")
	replicaID = flag.Uint64("replicaid", 1, "Replica ID to use")
)

func main() {
	flag.Parse()
	namenode.NewNameNodeServer(*addr, *replicaID).Setup(context.Background())
}
