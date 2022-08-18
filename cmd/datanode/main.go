package main

import "simple-distributed-storage-system/src/datanode"

const (
	dataNodeServerAddr = "localhost:9000"
)

func main() {
	datanode.Init(dataNodeServerAddr)
}
