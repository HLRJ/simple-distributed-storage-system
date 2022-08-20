package main

import (
	"context"
	"flag"
	"simple-distributed-storage-system/src/datanode"
)

var addr = flag.String("addr", "localhost:9000", "Node host address")

func main() {
	flag.Parse()
	datanode.NewDataNodeServer(*addr).Setup(context.Background())
}
