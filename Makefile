.PHONY: hello all clean

hello:
	go build -o bin/hello_server cmd/hello/server/main.go
	go build -o bin/hello_client cmd/hello/client/main.go

all:
	go build -o bin/namenode cmd/namenode/main.go
	go build -o bin/datanode cmd/datanode/main.go
	go build -o bin/client cmd/client/main.go

clean:
	rm bin/*
