---
{
"title": "构建 Docker Image",
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

<version since="1.2"></version>

# 构建 Docker Image 

该文档主要介绍了如何通过 Dockerfile 来制作 Apache Doris 的运行镜像，以便于在容器化编排工具或者快速测试过程中可迅速拉取一个 Apache Doris Image 来完成集群的创建。

## 软硬件要求

### 概述

Docker 镜像在制作前要提前准备好制作机器，该机器的平台架构决定了制作以后的 Docker Image 适用的平台架构，如 X86_64 机器，需要下载 X86_64 的 Doris 二进制程序，制作以后的 Image 仅可在 X86_64 平台上运行。ARM 平台（M1 视同为 ARM）同理。

### 硬件要求

最低配置：2C 4G

推荐配置：4C 16G

### 软件要求

Docker Version：20.10 及以后版本

## Docker Image 构建

Dockerfile 脚本编写需要注意以下几点：

> 1. 基础父镜像最好选用经过 Docker-Hub 认证的官方镜像，如有 `DOCKER OFFICIAL IMAGE` 标签的镜像提供者，一般是官方认证过没有问题的镜像源。
> 2. 基础父镜像选用时尽可能以能最小可用环境的原则进行选择，即选择时以满足基础必备环境的同时、镜像尽可能以最小的原则进行挑选，不要直接使用诸如完整的 CentOS 镜像、Ubuntu 镜像等提供完备功能的镜像，如果使用后者，则我们构建出的包会很大，不利于使用和传播，也会造成磁盘空间浪费。
> 3. 最好不要高度封装，若只是提供一个软件的原生镜像，最好的构建方式是 `最小可用环境` + `软件本身` ，期间不夹杂其他的逻辑和功能处理，这样可以更原子化的进行镜像编排以及后续的维护和更新操作。

由于 Apache Doris 1.2 版本开始，开始支持 JavaUDF 能力，故而 BE 也需要有 JDK 环境，推荐的镜像如下：

| Doris 程序 | 推荐基础父镜像    |
| ---------- | ----------------- |
| Frontend   | openjdk:8u342-jdk |
| Backend    | openjdk:8u342-jdk |

### 脚本前期准备

编译 Docker Image 的 Dockerfile 脚本中，关于 Apache Doris 程序二进制包的加载方式，有两种：

1. 通过 wget / curl 在编译时执行下载命令，随后完成 docker build 制作过程
2. 提前下载二进制包至编译目录，然后通过 ADD 或者 COPY 命令加载至 docker build 过程中

使用前者会让 Docker Image Size 更小，但是如果构建失败的话可能下载操作会重复进行，导致构建时间过长，而后者更适用于网络环境不是很好的构建环境。后者构建的镜像要略大于前者，但是不会大很多。

**综上，本文档的示例以第二种方式为准，若有第一种诉求，可根据自己需求定制修改即可。**

### 准备二进制包

需要注意的是，如有定制化开发需求，则需要自己修改源码后进行[编译](../source-install/compilation)打包，然后放置至构建目录即可。

若无特殊需求，直接[下载](https://doris.apache.org/zh-CN/download)官网提供的二进制包即可。

FE 和 BE 的构建目录

### 构建步骤

构建环境目录如下：

```sql
└── docker-build                                                // 构建根目录 
    ├── fe                                                      // FE 构建目录
    │   ├── dockerfile                                          // dockerfile 脚本
    │   └── resource                                            // 资源目录
    │       ├── run_fe.sh                                       // 启动及注册脚本
    │       └── apache-doris-x.x.x-bin-fe.tar.gz                // 二进制程序包
    └── be                                                      // BE 构建目录
        ├── dockerfile                                          // dockerfile 脚本
        └── resource                                            // 资源目录
            ├── run_be.sh                                       // 启动及注册脚本
            └── apache-doris-x.x.x-bin-x86_64/arm-be.tar.gz     // 二进制程序包
```

1. 创建构建环境目录

   ```shell
   mkdir -p ./docker-build/{fe,be}/resource
   ```

2. 下载[官方二进制包](https://doris.apache.org/zh-CN/download)/编译的二进制包

   拷贝二进制包至 `./docker-build/{fe,be}/resource` 目录下

3. 编写 FE 的 Dockerfile 脚本

   ```powershell
   # 选择基础镜像
   FROM openjdk:8u342-jdk
   
   # 设置环境变量
   ENV JAVA_HOME="/usr/local/openjdk-8/" \
       PATH="/opt/apache-doris/fe/bin:$PATH"
   
   # 下载软件至镜像内，可根据需要替换
   ADD ./resource/apache-doris-fe-${x.x.x}-bin.tar.gz /opt/
   
   RUN apt-get update && \
       apt-get install -y default-mysql-client && \
       apt-get clean && \
       mkdir /opt/apache-doris && \
       cd /opt && \
       mv apache-doris-fe-${x.x.x}-bin /opt/apache-doris/fe
   
   ADD ./resource/run_fe.sh /opt/apache-doris/fe/bin
   RUN chmod 755 /opt/apache-doris/fe/bin/run_fe.sh
   
   ENTRYPOINT ["/opt/apache-doris/fe/bin/run_fe.sh"]
   ```

   编写后命名为 `Dockerfile` 并保存至 `./docker-build/fe` 目录下

4. 编写 FE 的执行脚本

   可参考复制 [run_fe.sh](https://github.com/apache/doris/tree/master/docker/runtime/fe/resource/run_fe.sh) 的内容

   编写后命名为 `run_fe.sh` 并保存至 `./docker-build/fe/resouce` 目录下

5. 编写 BE 的 Dockerfile 脚本

   ```powershell
   # 选择基础镜像
   FROM openjdk:8u342-jdk
   
   # 设置环境变量
   ENV JAVA_HOME="/usr/local/openjdk-8/" \
       PATH="/opt/apache-doris/be/bin:$PATH"
   
   # 下载软件至镜像内，可根据需要替换
   ADD ./resource/apache-doris-be-${x.x.x}-bin-x86_64.tar.gz /opt/
   
   RUN apt-get update && \
       apt-get install -y default-mysql-client && \
       apt-get clean && \
       mkdir /opt/apache-doris && \
       cd /opt && \
       mv apache-doris-be-${x.x.x}-bin-x86_64 /opt/apache-doris/be
   
   ADD ./resource/run_be.sh /opt/apache-doris/be/bin
   RUN chmod 755 /opt/apache-doris/be/bin/run_be.sh
   
   ENTRYPOINT ["/opt/apache-doris/be/bin/run_be.sh"]
   ```

   编写后命名为 `Dockerfile` 并保存至 `./docker-build/be` 目录下

6. 编写 BE 的执行脚本

   可参考复制 [run_be.sh](https://github.com/apache/doris/tree/master/docker/runtime/be/resource/run_be.sh) 的内容

   编写后命名为 `run_be.sh` 并保存至 `./docker-build/be/resouce` 目录下

7. 执行构建

   需要注意的是，`${tagName}` 需替换为你想要打包命名的 tag 名称，如：`apache-doris:1.1.3-fe`

   构建 FE：

   ```shell
   cd ./docker-build/fe
   docker build . -t ${fe-tagName}
   ```

   构建 BE：

   ```shell
   cd ./docker-build/be
   docker build . -t ${be-tagName}
   ```

   构建完成后，会有 `Success` 字样提示，这时候通过以下命令可查看刚构建完成的 Image 镜像

   ```shell
   docker images
   ```

## 部署流程

该章节部署仅示例 1FE 1BE 部署流程，为减少文档重复性描述，HA 部署及 NBE 部署流程详解见 [Doris Docker Compose](./doris_docker_compose)

部署命令中，如果没有准备持久化容器数据的目的，可不使用 `-v` 参数直接启动

### 单节点部署

首先可以创建一个网桥来用于 Doris 集群的部署，防止网络问题，此处使用桥接网络模式

```shell
# 创建指定网桥
docker network create --subnet={xxx.xxx.xxx.0}/24 dorisNetwork
# 示例
docker network create --subnet=172.20.80.0/24 dorisNetwork
```

使用自定义网桥创建的容器 IP 都将在该网段下，也可以手动指定

创建容器，可根据数据卷挂载来完成数据的持久化操作

```shell
# 创建 FE 容器
docker run \
-itd \
--name=doris-fe \
-p ${宿主机端口}:8030 \
-p ${宿主机端口}:9010 \
-p ${宿主机端口}:9030 \
-v ${宿主机 FE 配置目录}:/opt/apache-doris/fe/conf/ \
-v ${宿主机 FE 数据日志目录}:/opt/apache-doris/fe/log/ \
-v ${宿主机 FE 元数据目录}:/opt/apache-doris/fe/doris-meta/ \
--network=dorisNetwork \
--ip ${fe_addr} \
${fe-tagName} \
--fe_id=1 \
--fe_servers="${fe_name}:${fe_addr}:${fe_editLogPort}"

# 创建 BE 容器
docker run \
-itd \
--name=doris-be \
-p ${宿主机端口}:8040 \
-p ${宿主机端口}:9000 \
-p ${宿主机端口}:9050 \
-v ${宿主机 BE 配置目录}:/opt/apache-doris/be/conf/ \
-v ${宿主机 BE 数据日志目录}:/opt/apache-doris/be/log/ \
-v ${宿主机 BE 数据目录}:/opt/apache-doris/be/storage/ \
--network=dorisNetwork \
--ip ${be_addr} \
${be-tagName} \
--fe_servers="${fe_name}:${fe_addr}:${fe_editLogPort}" \
--be_addr="${be_addr}:${be_heartPort}"
```

启动后可使用 `docker logs ${container_name}` 来查看启动日志

### 多节点部署

多节点部署的前提是节点之间需要**内网互通**，该示例将使用 HOST 网络模式来进行部署

A 节点部署 FE

```shell
# 创建 FE 容器
docker run \
-itd \
--name=doris-fe \
--net=host \
-v ${宿主机 FE 配置目录}:/opt/apache-doris/fe/conf/ \
-v ${宿主机 FE 数据日志目录}:/opt/apache-doris/fe/log/ \
-v ${宿主机 FE 元数据目录}:/opt/apache-doris/fe/doris-meta/ \
${fe-tagName} \
--fe_id=1 \
--fe_servers="${fe_name}:${fe_addr}:${fe_editLogPort}"
```

B 节点部署 BE

```shell
# 创建 BE 容器
docker run \
-itd \
--name=doris-be \
--net=host \
-v ${宿主机 BE 配置目录}:/opt/apache-doris/be/conf/ \
-v ${宿主机 BE 数据日志目录}:/opt/apache-doris/be/log/ \
-v ${宿主机 BE 数据目录}:/opt/apache-doris/be/storage/ \
${be-tagName} \
--fe_servers="${fe_name}:${fe_addr}:${fe_editLogPort}" \
--be_addr="${be_addr}:${be_heartPort}"
```

同上，启动后可使用 `docker logs ${container_name}` 来查看启动日志

## 推送镜像至 DockerHub 或私有仓库

登录 DockerHub 账号

```
docker login
```

登录成功会提示 `Success` 相关提示，随后推送至仓库即可

```shell
docker push ${tagName}
```
