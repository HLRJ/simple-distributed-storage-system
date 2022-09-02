# simple-distributed-storage-system

## report

[2022 年字节青训营大数据专场结营项目三 - Rabbit 队 - 简易分布式存储系统实现](https://bytedancecampus1.feishu.cn/docx/doxcnfVgtyPjujq8sB1knLhfouf)

## quick start

### prerequisite

- docker
- protobuf
  - for Arch Linux, just `yay -S protoc-gen-go protoc-gen-go-grpc`

### setup server

```
make setup
```

it will:
1. run the script to generate files for protobuf
2. go build to generate executable file
3. run zipkin in docker for distributed tracing
4. start the namenode and datanode servers

### setup client

```
make SDSS-ctl
./bin/SDSS-ctl List /
./bin/SDSS-ctl Mkdir /doc/
./bin/SDSS-ctl Put LICENSE /doc/LICENSE
./bin/SDSS-ctl Get /doc/LICENSE /tmp/LICENSE
diff /tmp/LICENSE LICENSE
./bin/SDSS-ctl Stat /doc/LICENSE
```

access http://127.0.0.1:9411/zipkin to see the visual RPC communication between servers

### kill server

```
make kill
```

it will:
- kill and remove the zipkin docker container
- kill all the namenode and datanode servers

## docker

build images for namenode and datanode

```
docker build -f docker/namenode/Dockerfile -t vgalaxy/namenode .
docker build -f docker/datanode/Dockerfile -t vgalaxy/datanode .
```

start the namenode and datanode servers in host network

```
docker run -d --network=host vgalaxy/namenode -addr localhost:8000 -replicaid 1
docker run -d --network=host vgalaxy/namenode -addr localhost:8001 -replicaid 2
docker run -d --network=host vgalaxy/namenode -addr localhost:8002 -replicaid 3

docker run -d --network=host vgalaxy/datanode -addr localhost:9000
docker run -d --network=host vgalaxy/datanode -addr localhost:9001
docker run -d --network=host vgalaxy/datanode -addr localhost:9002
```

## test

**Before running the tests, zipkin should be set up for distributed tracing.**

## contributing

see [how to pull requests](https://docs.github.com/en/pull-requests)
- [HLRJ](https://github.com/HLRJ)
- [V_Galaxy](https://github.com/VGalaxies)
- [jinbo-self](https://github.com/jinbo-self)

Thanks to all the people who already contributed!

## license

[SDSS](https://github.com/HLRJ/simple-distributed-storage-system) is under the MIT License. See the [LICENSE](LICENSE) file for details.