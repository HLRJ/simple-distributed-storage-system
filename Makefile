.PHONY: all hello SDSS-ctl proto clean

SDSS-ctl:
	go build -o output/SDSS-ctl cmd/ctl/main.go

all: proto
	go build -o bin/namenode cmd/namenode/main.go
	go build -o bin/datanode cmd/datanode/main.go
	go build -o bin/client cmd/client/main.go

hello: proto
	go build -o bin/hello_server cmd/hello/server/main.go
	go build -o bin/hello_client cmd/hello/client/main.go

SDSS-ctl:
	go build -o output/SDSS-ctl cmd/ctl/main.go

proto:
	go generate ./src/protos/gen.go

clean:
	rm bin/*
