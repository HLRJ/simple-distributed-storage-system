package main

import (
	"context"
	"simple-distributed-storage-system/src/namenode"
)

func main() {
	namenode.NewNameNodeServer().Setup(context.Background())
}
