# simple-distributed-storage-system

[2022年字节青训营大数据专场结营项目三   Rabbit队 简易分布式存储系统实现](https://bytedancecampus1.feishu.cn/docx/doxcnfVgtyPjujq8sB1knLhfouf)

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

## zipkin

```
docker run -d -p 9411:9411 openzipkin/zipkin
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

## doc

### basic idea

- client 与 namenode 交互，确定数据的存储分布，datanode 提供基本的 KV 存储抽象
- namenode 之间通过 raft 协议达成元数据一致性，其 leader 节点负责写请求，其余节点仅可以负责读请求
- datanode 之间不存在通信

### write process

1. client 向 namenode 请求创建一个文件
2. namenode 将文件分块，设置数据结构，并返回块的大小
3. client 以块为单位，向 namenode 请求该块应存储的 datanode 地址
4. client 向返回的 datanode 发起写请求，并记录请求结果
5. client 通知 namenode 写入的结果

### read process

1. client 向 namenode 请求打开一个文件
2. namenode 返回块的大小和个数
3. client 以块为单位，向 namenode 请求该块应存储的 datanode 地址
4. client 向返回的 datanode 发起读请求

## limitations

### remove

- 文件 remove 后，其元数据仍保留，这代表无法创建 remove 的同名文件
- 无法 remove 目录

### file system

- 使用线性数据结构模拟目录树
- 目录必须以 `/` 结尾

### data distribution and migration

- 在创建文件时，必须有 `replicaFactor` 个注册的 datanode，否则会创建失败
- namenode 定期检查全体 datanode 的心跳
  - 若发现 datanode 离线，则对该 datanode 对应的 loc 进行数据迁移
  - 若该 loc 对应的一个 block 数据迁移失败，则认为整体数据迁移失败
- 当 datanode 重新注册时，namenode 会认为该 datanode 之前所对应 loc 的数据全部丢失
  - 这也是引入 loc 作为虚拟地址的原因
- 上述对 datanode 的选择均采取随机的方式

### connection

- client 仅会在调用服务前检查与现有 namenode 的连接，在调用过程中若连接中断则无法切换

## contributing
 see [how to pull requests](https://docs.github.com/en/pull-requests)
- [HLRJ](https://github.com/HLRJ)
- [V_Galaxy](https://github.com/VGalaxies)
- [jinbo-self](https://github.com/jinbo-self)

Thanks to all the people who already contributed!

## license
[SDSS](https://github.com/HLRJ/simple-distributed-storage-system) is under the MIT License. See the [LICENSE](LICENSE) file for details.