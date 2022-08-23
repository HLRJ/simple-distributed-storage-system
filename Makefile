.PHONY: all zipkin setup kill-zipkin kill SDSS-ctl server hello proto clean-data clean

all: SDSS-ctl server

zipkin:
	docker run -d -p 9411:9411 openzipkin/zipkin

setup: clean-data zipkin server
	./bin/namenode -addr localhost:8000 -replicaid 1 &
	./bin/namenode -addr localhost:8001 -replicaid 2 &
	./bin/namenode -addr localhost:8002 -replicaid 3 &
	./bin/datanode -addr localhost:9000 &
	./bin/datanode -addr localhost:9001 &
	./bin/datanode -addr localhost:9002 &

kill-zipkin:
	docker kill `docker ps -aq --filter ancestor=openzipkin/zipkin`
	docker rm `docker ps -aq --filter ancestor=openzipkin/zipkin`

kill: kill-zipkin
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

clean-data:
	rm -rf data

clean: clean-data
	rm -rf bin
