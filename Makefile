.PHONY: all setup kill SDSS-ctl server hello proto clean

all: SDSS-ctl server

setup: server
	./bin/namenode -addr localhost:8000 -replicaid 1 &
	./bin/namenode -addr localhost:8001 -replicaid 2 &
	./bin/namenode -addr localhost:8002 -replicaid 3 &
	./bin/datanode -addr localhost:9000 &
	./bin/datanode -addr localhost:9001 &
	./bin/datanode -addr localhost:9002 &

kill:
	pkill namenode
	pkill datanode

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
	rm -rf bin data
