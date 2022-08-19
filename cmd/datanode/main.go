package main

import (
	"context"
	"flag"
	"simple-distributed-storage-system/src/datanode"
)

var dataNodeServerAddr = flag.String("addr", "localhost:9000", "input this datanode address")

func main() {
	flag.Parse()
	datanode.NewDataNodeServer(*dataNodeServerAddr).Setup(context.Background())
}
