package main

import "simple-distributed-storage-system/src/client"

func main() {
	c := client.NewClient()
	c.Put("/tmp/README.md", "/doc/README.md")
	c.CloseClient()
}
