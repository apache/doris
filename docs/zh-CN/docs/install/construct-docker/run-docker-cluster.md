---
{
"title": "部署 Docker 集群",
"language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# 部署 Docker 集群

## 背景说明

本篇将简述如何通过 `docker run` 或 `docker-compose up` 命令快速构建一套完整的 Doris 测试集群。

## 适用场景

建议在 SIT 或者 DEV 环境中使用 Doris Docker 来简化部署的流程。

如在新版本中想测试某一个功能点，可以使用 Doris Docker 部署一个 Playground 环境。或者在调试的过程中要复现某个问题时，也可以使用 docker 环境来模拟。

在生产环境上，当前暂时尽量避免使用容器化的方案进行 Doris 部署。

## 软件环境

| 软件           | 版本        |
| -------------- | ----------- |
| Docker         | 20.0 及以上 |
| docker-compose | 2.10 及以上 |

## 硬件环境

| 配置类型 | 硬件信息 | 最大运行集群规模 |
| -------- | -------- | ---------------- |
| 最低配置 | 2C 4G    | 1FE 1BE          |
| 推荐配置 | 4C 16G   | 3FE 3BE          |

## 前期环境准备

需在宿主机执行如下命令

```shell
sysctl -w vm.max_map_count=2000000
```

## Docker Compose

不同平台需要使用不同 Image 镜像，本篇以 `X86_64` 平台为例。

### 网络模式说明

Doris Docker 适用的网络模式有两种。

1. 适合跨多节点部署的 HOST 模式，这种模式适合每个节点部署 1FE 1BE。
2. 适合单节点部署多 Doris 进程的子网网桥模式，这种模式适合单节点部署（推荐），若要多节点混部需要做更多组件部署（不推荐）。

为便于展示，本章节仅演示子网网桥模式编写的脚本。

### 接口说明

从 `Apache Doris 1.2.1 Docker Image` 版本起，各个进程镜像接口列表如下：

| 进程名    | 接口名         | 接口定义          | 接口示例             |
|--------|-------------|---------------|------------------|
| FE     | BE          | BROKER        | FE_SERVERS       | FE 节点主要信息     | fe1:172.20.80.2:9010,fe2:172.20.80.3:9010,fe3:172.20.80.4:9010 |
| FE     | FE_ID       | FE 节点 ID      | 1                |
| BE     | BE_ADDR     | BE 节点主要信息     | 172.20.80.5:9050 |
| BE     | NODE_ROLE   | BE 节点类型       | computation      |
| BROKER | BROKER_ADDR | BROKER 节点主要信息 | 172.20.80.6:8000 |

注意，以上接口必须填写信息，否则进程无法启动。

> FE_SERVERS 接口规则为：`FE_NAME:FE_HOST:FE_EDIT_LOG_PORT[,FE_NAME:FE_HOST:FE_EDIT_LOG_PORT]`
>
> FE_ID 接口规则为：`1-9` 的整数，其中 `1` 号 FE 为 Master 节点。
>
> BE_ADDR 接口规则为：`BE_HOST:BE_HEARTBEAT_SERVICE_PORT`
>
> NODE_ROLE 接口规则为：`computation` 或为空，其中为空或为其他值时表示节点类型为 `mix` 类型
>
> BROKER_ADDR 接口规则为：`BROKER_HOST:BROKER_IPC_PORT`

### 脚本模板

#### Docker Run 命令

1FE & 1BE 命令模板

注意需要修改 `${当前机器的内网IP}` 替换为当前机器的内网IP

```shell 
docker run -itd \
--name=fe \
--env FE_SERVERS="fe1:${当前机器的内网IP}:9010" \
--env FE_ID=1 \
-p 8030:8030 \
-p 9030:9030 \
-v /data/fe/doris-meta:/opt/apache-doris/fe/doris-meta \
-v /data/fe/log:/opt/apache-doris/fe/log \
--net=host \
apache/doris:2.0.0_alpha-fe-x86_64

docker run -itd \
--name=be \
--env FE_SERVERS="fe1:${当前机器的内网IP}:9010" \
--env BE_ADDR="${当前机器的内网IP}:9050" \
-p 8040:8040 \
-v /data/be/storage:/opt/apache-doris/be/storage \
-v /data/be/log:/opt/apache-doris/be/log \
--net=host \
apache/doris:2.0.0_alpha-be-x86_64
```

3FE & 3BE Run 命令模板如有需要[点击此处](https://github.com/apache/doris/tree/master/docker/runtime/docker-compose-demo/build-cluster/rum-command/3fe_3be.sh)访问下载。

#### Docker Compose 脚本

1FE & 1BE 模板

注意需要修改 `${当前机器的内网IP}` 替换为当前机器的内网IP

``` yaml
version: "3"
services:
  fe:
    image: apache/doris:2.0.0_alpha-fe-x86_64
    hostname: fe
    environment:
     - FE_SERVERS=fe1:${当前机器的内网IP}:9010
     - FE_ID=1
    volumes:
     - /data/fe/doris-meta/:/opt/apache-doris/fe/doris-meta/
     - /data/fe/log/:/opt/apache-doris/fe/log/
    network_mode: host
  be:
    image: apache/doris:2.0.0_alpha-be-x86_64
    hostname: be
    environment:
     - FE_SERVERS=fe1:${当前机器的内网IP}:9010
     - BE_ADDR=${当前机器的内网IP}:9050
    volumes:
     - /data/be/storage/:/opt/apache-doris/be/storage/
     - /data/be/script/:/docker-entrypoint-initdb.d/
    depends_on:
      - fe
    network_mode: host
```

3FE & 3BE Docker Compose 脚本模板如有需要[点击此处](https://github.com/apache/doris/tree/master/docker/runtime/docker-compose-demo/build-cluster/docker-compose/3fe_3be/docker-compose.yaml)访问下载。

## 部署 Doris Docker

部署方式二选一即可：

1. 执行 `docker run` 命令创建集群
2. 保存 `docker-compose.yaml` 脚本，同目录下执行 `docker-compose up -d` 命令创建集群

### 特例说明

MacOS 由于内部实现容器的方式不同，在部署时宿主机直接修改 `max_map_count` 值可能无法成功，需要先创建以下容器：

```shel
docker run -it --privileged --pid=host --name=change_count debian nsenter -t 1 -m -u -n -i sh
```

容器创建成功执行以下命令：

```shell
sysctl -w vm.max_map_count=2000000
```

然后 `exit` 退出，创建 Doris Docker 集群。


