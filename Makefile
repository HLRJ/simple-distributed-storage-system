.PHONY: server hello SDSS-ctl proto clean

SDSS-ctl: proto
	go build -o bin/SDSS-ctl cmd/ctl/main.go

server: proto
	go build -o bin/namenode cmd/namenode/main.go
	go build -o bin/datanode cmd/datanode/main.go

hello: proto
	go build -o bin/hello_server cmd/hello/server/main.go
	go build -o bin/hello_client cmd/hello/client/main.go

proto:
	go generate ./src/protos/gen.go

clean:
	rm bin/*
