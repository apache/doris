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

> 1. 基础父镜像选用经过 Docker-Hub 认证的 OpenJDK 官方镜像，版本用 JDK 1.8 版本
> 2. 应用程序默认使用官方提供的二进制包进行下载，勿使用来源不明的二进制包
> 3. 需要内嵌脚本来完成 FE 的启动、多 FE 注册、状态检查和 BE 的启动、注册 BE 至 FE 、状态检查等任务流程
> 4. 应用程序在 Docker 内启动时不应使用 `--daemon` 的方式启动，否则在 K8S 等编排工具部署过程中会有异常

由于 Apache Doris 1.2 版本开始，开始支持 JavaUDF 能力，故而 BE 也需要有 JDK 环境，推荐的镜像如下：

| Doris 程序 | 推荐基础父镜像    |
| ---------- | ----------------- |
| Frontend   | openjdk:8u342-jdk |
| Backend    | openjdk:8u342-jdk |
| Broker     | openjdk:8u342-jdk |

### 脚本前期准备

编译 Docker Image 的 Dockerfile 脚本中，关于 Apache Doris 程序二进制包的加载方式，有两种：

1. 通过 wget / curl 在编译时执行下载命令，随后完成 docker build 制作过程
2. 提前下载二进制包至编译目录，然后通过 ADD 或者 COPY 命令加载至 docker build 过程中

使用前者会让 Docker Image Size 更小，但是如果构建失败的话可能下载操作会重复进行，导致构建时间过长，而后者更适用于网络环境不是很好的构建环境。

**综上，本文档的示例以第二种方式为准，若有第一种诉求，可根据自己需求定制修改即可。**

### 准备二进制包

需要注意的是，如有定制化开发需求，则需要自己修改源码后进行[编译](../source-install/compilation-general.md)打包，然后放置至构建目录即可。

若无特殊需求，直接[下载](https://doris.apache.org/zh-CN/download)官网提供的二进制包即可。

### 构建步骤

#### 构建 FE

构建环境目录如下：

```sql
└── docker-build                                                // 构建根目录 
    └── fe                                                      // FE 构建目录
        ├── dockerfile                                          // dockerfile 脚本
        └── resource                                            // 资源目录
            ├── init_fe.sh                                      // 启动及注册脚本
            └── apache-doris-x.x.x-bin-fe.tar.gz                // 二进制程序包
```

1. 创建构建环境目录

   ```shell
   mkdir -p ./docker-build/fe/resource
   ```

2. 下载[官方二进制包](https://doris.apache.org/zh-CN/download)/编译的二进制包

   拷贝二进制包至 `./docker-build/fe/resource` 目录下

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
   
   ADD ./resource/init_fe.sh /opt/apache-doris/fe/bin
   RUN chmod 755 /opt/apache-doris/fe/bin/init_fe.sh
   
   ENTRYPOINT ["/opt/apache-doris/fe/bin/init_fe.sh"]
   ```

   编写后命名为 `Dockerfile` 并保存至 `./docker-build/fe` 目录下

4. 编写 FE 的执行脚本

   可参考复制 [init_fe.sh](https://github.com/apache/doris/tree/master/docker/runtime/fe/resource/init_fe.sh) 的内容

   编写后命名为 `init_fe.sh` 并保存至 `./docker-build/fe/resource` 目录下

5. 执行构建

   需要注意的是，`${tagName}` 需替换为你想要打包命名的 tag 名称，如：`apache-doris:1.1.3-fe`

   构建 FE：

   ```shell
   cd ./docker-build/fe
   docker build . -t ${fe-tagName}
   ```


#### 构建 BE

1. 创建构建环境目录

```shell
mkdir -p ./docker-build/be/resource
```
2. 构建环境目录如下：

   ```sql
   └── docker-build                                                // 构建根目录 
       └── be                                                      // BE 构建目录
           ├── dockerfile                                          // dockerfile 脚本
           └── resource                                            // 资源目录
               ├── init_be.sh                                      // 启动及注册脚本
               └── apache-doris-x.x.x-bin-x86_64/arm-be.tar.gz     // 二进制程序包
   ```

3. 编写 BE 的 Dockerfile 脚本

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
   
   ADD ./resource/init_be.sh /opt/apache-doris/be/bin
   RUN chmod 755 /opt/apache-doris/be/bin/init_be.sh
   
   ENTRYPOINT ["/opt/apache-doris/be/bin/init_be.sh"]
   ```

   编写后命名为 `Dockerfile` 并保存至 `./docker-build/be` 目录下

4. 编写 BE 的执行脚本

   可参考复制 [init_be.sh](https://github.com/apache/doris/tree/master/docker/runtime/be/resource/init_be.sh) 的内容

   编写后命名为 `init_be.sh` 并保存至 `./docker-build/be/resource` 目录下

5. 执行构建

   需要注意的是，`${tagName}` 需替换为你想要打包命名的 tag 名称，如：`apache-doris:1.1.3-be`

   构建 BE：

   ```shell
   cd ./docker-build/be
   docker build . -t ${be-tagName}
   ```

   构建完成后，会有 `Success` 字样提示，这时候通过以下命令可查看刚构建完成的 Image 镜像

   ```shell
   docker images
   ```

#### 构建 Broker

1.  创建构建环境目录

```shell
mkdir -p ./docker-build/broker/resource
```

2. 构建环境目录如下：

   ```sql
   └── docker-build                                                // 构建根目录 
       └── broker                                                  // BROKER 构建目录
           ├── dockerfile                                          // dockerfile 脚本
           └── resource                                            // 资源目录
               ├── init_broker.sh                                  // 启动及注册脚本
               └── apache-doris-x.x.x-bin-broker.tar.gz            // 二进制程序包
   ```

3. 编写 Broker 的 Dockerfile 脚本

   ```powershell
   # 选择基础镜像
   FROM openjdk:8u342-jdk
   
   # 设置环境变量
   ENV JAVA_HOME="/usr/local/openjdk-8/" \
       PATH="/opt/apache-doris/broker/bin:$PATH"
   
   # 下载软件至镜像内，此处 broker 目录被同步压缩至 FE 的二进制包，需要自行解压重新打包，可根据需要替换
   ADD ./resource/apache_hdfs_broker.tar.gz /opt/
   
   RUN apt-get update && \
       apt-get install -y default-mysql-client && \
       apt-get clean && \
       mkdir /opt/apache-doris && \
       cd /opt && \
       mv apache_hdfs_broker /opt/apache-doris/broker
   
   ADD ./resource/init_broker.sh /opt/apache-doris/broker/bin
   RUN chmod 755 /opt/apache-doris/broker/bin/init_broker.sh
   
   ENTRYPOINT ["/opt/apache-doris/broker/bin/init_broker.sh"]
   ```

   编写后命名为 `Dockerfile` 并保存至 `./docker-build/broker` 目录下

4. 编写 BE 的执行脚本

   可参考复制 [init_broker.sh](https://github.com/apache/doris/tree/master/docker/runtime/broker/resource/init_broker.sh) 的内容

   编写后命名为 `init_broker.sh` 并保存至 `./docker-build/broker/resource` 目录下

5. 执行构建

   需要注意的是，`${tagName}` 需替换为你想要打包命名的 tag 名称，如：`apache-doris:1.1.3-broker`

   构建 Broker：

   ```shell
   cd ./docker-build/broker
   docker build . -t ${broker-tagName}
   ```

   构建完成后，会有 `Success` 字样提示，这时候通过以下命令可查看刚构建完成的 Image 镜像

   ```shell
   docker images
   ```

## 推送镜像至 DockerHub 或私有仓库

登录 DockerHub 账号

```
docker login
```

登录成功会提示 `Success` 相关提示，随后推送至仓库即可

```shell
docker push ${tagName}
```
