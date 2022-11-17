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

> 1. 基础父镜像最好选用经过 Docker-Hub 认证的官方镜像，如有 `DOCKER OFFICIAL IMAGE` 标签的镜像提供者，一般是官方认证过没有问题的镜像源。
> 2. 基础父镜像选用时尽可能以能最小可用环境的原则进行选择，即选择时以满足基础必备环境的同时、镜像尽可能以最小的原则进行挑选，不要直接使用诸如完整的 CentOS 镜像、Ubuntu 镜像等提供完备功能的镜像，如果使用后者，则我们构建出的包会很大，不利于使用和传播，也会造成磁盘空间浪费。
> 3. 最好不要高度封装，若只是提供一个软件的原生镜像，最好的构建方式是 `最小可用环境` + `软件本身` ，期间不夹杂其他的逻辑和功能处理，这样可以更原子化的进行镜像编排以及后续的维护和更新操作。

其中推荐的镜像如下：

| Doris 程序 | 推荐基础父镜像         |
| ---------- | ---------------------- |
| Frontend   | openjdk:8u342-jdk      |
| Backend    | bitnami/minideb:latest |

### 脚本前期准备

编译 Docker Image 的 Dockerfile 脚本中，关于 Apache Doris 程序二进制包的加载方式，有两种：

1. 通过 wget / curl 在编译时执行下载命令，随后完成 docker build 制作过程
2. 提前下载二进制包至编译目录，然后通过 ADD 或者 COPY 命令加载至 docker build 过程中

使用前者会让 Docker Image Size 更小，但是如果构建失败的话可能下载操作会重复进行，导致构建时间过长，而后者更适用于网络环境不是很好的构建环境。后者构建的镜像要略大于前者，但是不会大很多。

**综上，本文档的示例以第二种方式为准，若有第一种诉求的，可根据自己需求定制修改即可。**

### 准备二进制包

需要注意的是，如有定制化开发需求，则需要自己修改源码后进行[编译](./source-install/compilation)打包，然后放置至构建目录即可。

若无特殊需求，直接[下载](https://doris.apache.org/zh-CN/download)官网提供的二进制包即可。

FE 和 BE 的构建目录

### 构建步骤

构建环境目录如下：

```sql
└── docker-build                                                // 构建根目录 
    ├── fe                                                      // FE 构建目录
    │   ├── dockerfile                                          // dockerfile 脚本
    │   └── resource                                            // 资源目录
    │       ├── other.sh                                        // 其他的操作脚本（无必须可忽略）
    │       └── apache-doris-x.x.x-bin-fe.tar.gz                // 二进制程序包
    └── be                                                      // BE 构建目录
        ├── dockerfile                                          // dockerfile 脚本
        └── resource                                            // 资源目录
            ├── other.sh                                        // 其他的操作脚本（无必须可忽略）
            └── apache-doris-x.x.x-bin-x86_64/arm-be.tar.gz     // 二进制程序包
```

1. 创建构建环境目录

   ```shell
   mkdir -p ./docker-build/{fe,be}/resource
   ```

2. 下载二进制包

   ```shell
   # 下载 FE 二进制包
   cd ./docker-build/fe/resource
   wget https://mirrors.tuna.tsinghua.edu.cn/apache/doris/x.x/x.x.x-rc0x/apache-doris-fe-x.x.x-bin.tar.gz
   # 下载 BE 二进制包
   cd ../../be/resource
   wget https://mirrors.tuna.tsinghua.edu.cn/apache/doris/x.x/x.x.x-rc0x/apache-doris-be-x.x.x-bin-x86_64/arm.tar.gz
   ```

3. 编写 Dockerfile-FE 脚本

   ```powershell
   # 选择基础镜像
   FROM openjdk:8u342-jdk
   
   # 根据自己需求进行替换，路径使用相对路径
   ARG package_url=本地二进制包路径，与 package_name 拼接得到完整目录
   # 例如 ARG package_url=./resource
   
   ARG package_name=二进制包文件名
   # 例如 ARG package_name=apache-doris-1.1.3-bin-fe.tar.gz
   
   ARG package_path=解压后文件目录名
   # 例如 ARG package_path=apache-doris-1.1.3-bin-fe
   
   # 设置环境变量
   ENV JAVA_HOME="/usr/local/openjdk-8/" \
       PATH="/usr/local/apache-doris/fe/bin:$PATH"
       
   # 下载软件至镜像内，可根据需要替换
   COPY $package_url/$package_name /usr/local/
   
   # 部署软件
   RUN tar -zxvf /usr/local/$package_name -C /usr/local/ && \
       mv /usr/local/$package_path /usr/local/apache-doris && \
       rm -rf /usr/local/$package_name /usr/local/apache-doris/be /usr/local/apache-doris/udf /usr/local/apache-doris/apache_hdfs_broker
   
   CMD ["bash"]
   ```

   编写后命名为 `dockerfile` 并保存至 `./docker-build/fe` 目录下

4. 编写 Dockerfile-BE 脚本

   ```powershell
   # 选择基础镜像
   FROM bitnami/minideb:latest
   
   # 根据自己需求进行替换
   ARG package_url=本地二进制包路径，与 package_name 拼接得到完整目录
   # 例如 ARG package_url=./resource
   
   ARG package_name=二进制包文件名
   # 例如 ARG package_name=apache-doris-1.1.3-bin-be-x86_64.tar.gz
   
   ARG package_path=解压后文件目录名
   # 例如 ARG package_path=apache-doris-1.1.3-bin-be-x86_64
   
   # 设置环境变量
   ENV PATH="/usr/local/apache-doris/be/bin:/usr/local/apache-doris/apache_hdfs_broker/bin:$PATH"
   
   # 下载软件至镜像内，可根据需要替换
   COPY $package_url/$package_name /usr/local/
   
   # 部署软件
   RUN tar -zxvf /usr/local/$package_name -C /usr/local/ && \
       mv /usr/local/$package_path /usr/local/apache-doris && \
       rm -rf /usr/local/$package_name /usr/local/apache-doris/fe /usr/local/apache-doris/udf
   
   CMD ["bash"]
   ```

   编写后命名为 `dockerfile` 并保存至 `./docker-build/be` 目录下

5. 执行构建

   需要注意的是，`${tagName}` 需替换为你想要打包命名的 tag 名称，如：`apache-doris:1.1.3-fe`

   构建 FE：

   ```shell
   cd ./docker-build/fe
   docker build . -t ${tagName}
   ```

   构建 BE：

   ```shell
   cd ./docker-build/be
   docker build . -t ${tagName}
   ```

### 构建完成并验证

构建完成后，会有 `Success` 字样提示，这时候通过以下命令可查看刚构建完成的 Image 镜像

```shell
docker images
```

首先可以创建一个网桥来用于 Doris 集群的部署，防止网络问题

```shell
# 创建指定网桥
docker network create --subnet=172.20.80.0/24 dorisNetwork
```

可以通过以下命令来创建一个 FE 容器和一个 BE 容器来测试构建是否成功

```shell
# 创建 FE 容器
docker run -itd -p 8030:8030 -p 9030:9030 --network=dorisNetwork --ip 172.20.80.2 --name=doris-fe ${tagName}
# 进入 FE 容器
docker exec -it doris-fe bash
# 进入容器后修改 fe.conf 的 IP 配置
perl -pi -e "s|# priority_networks = 10.10.10.0/24;192.168.0.0/16|priority_networks = 172.20.80.0/16|g" /usr/local/apache-doris/fe/conf/fe.conf
# 启动 Doris-FE 进程
start_fe.sh --daemon
# 退出容器
exit
```

进程是否成功，可根据[部署章节](./install-deploy)的常见问题-进程相关来进行对应测试

```shell
# 创建 BE 容器
docker run -itd -p 8030:8030 -p 9030:9030 --network=dorisNetwork --ip 172.20.80.3 --name=doris-be ${tagName}
# 进入 BE 容器
docker exec -it doris-be bash
# 进入容器后修改 fe.conf 的 IP 配置
perl -pi -e "s|# priority_networks = 10.10.10.0/24;192.168.0.0/16|priority_networks = 172.20.80.0/16|g" /usr/local/apache-doris/be/conf/be.conf
# 启动 Doris-BE 进程
start_be.sh --daemon
# 退出容器
exit
```

启动都无问题后，可通过 MySQL-Client 客户端或者 FE-Web-UI 客户端来完成 BE 注册至 FE 的工作，1FE 1BE 的最小 Doris 集群即部署完成。

### 推送镜像至 DockerHub 或私有仓库

登录 DockerHub 账号

```
docker login
```

登录成功会提示 `Success` 相关提示，随后推送至仓库即可

```shell
docker push ${tagName}
```
