# simple-distributed-storage-system

2022年字节青训营大数据专场结营项目三——简易分布式存储系统实现

## protobuf

```
yay -S protoc-gen-go protoc-gen-go-grpc
make proto
```

## client

```
make SDSS-ctl
./bin/SDSS-ctl
```

## server

```
make server
make setup
make kill
```

## docker

```
docker build -f docker/namenode/Dockerfile -t vgalaxy/namenode .
docker build -f docker/datanode/Dockerfile -t vgalaxy/datanode .

docker run -d --network=host vgalaxy/namenode -addr localhost:8000 -replicaid 1
docker run -d --network=host vgalaxy/namenode -addr localhost:8001 -replicaid 2
docker run -d --network=host vgalaxy/namenode -addr localhost:8002 -replicaid 3

docker run -d --network=host vgalaxy/datanode -addr localhost:9000
docker run -d --network=host vgalaxy/datanode -addr localhost:9001
docker run -d --network=host vgalaxy/datanode -addr localhost:9002
```