---
{
"title": "在 Windows 平台上编译",
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

# 在 Windows 平台上编译

本文介绍如何在 Windows 平台上编译源码

## 环境要求

1. Windows 11 或 Windows 10 版本 1903、内部版本 18362 或更高版本中可用
2. 可正常使用 WSL2，WSL2 开启步骤不再在此赘述

## 编译步骤

1. 通过 Microsoft Store 安装 Oracle Linux 7.9 发行版

   > 也可通过 Docker 镜像或 Github 安装方式安装其他想要的发行版

2. 打开 CMD，指定身份运行 WSL2

   ```shell
   wsl -d OracleLinux_7_9 -u root
   ```

3. 安装依赖

   ```shell
   # install required system packages
   sudo yum install -y byacc patch automake libtool make which file ncurses-devel gettext-devel unzip bzip2 zip util-linux wget git python2
   
   # install autoconf-2.69
   wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz && \
       tar zxf autoconf-2.69.tar.gz && \
       cd autoconf-2.69 && \
       ./configure && \
       make && \
       make install
   
   # install bison-3.0.4
   wget http://ftp.gnu.org/gnu/bison/bison-3.0.4.tar.gz && \
       tar xzf bison-3.0.4.tar.gz && \
       cd bison-3.0.4 && \
       ./configure && \
       make && \
       make install
   ```

4. 安装 LDB_TOOLCHAIN 及其他主要编译环境

    - [Java8](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u131-linux-x64.tar.gz)
    - [Apache Maven 3.6.3](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/apache-maven-3.6.3-bin.tar.gz)
    - [Node v12.13.0](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v12.13.0-linux-x64.tar.gz)
    - [LDB_TOOLCHAIN](https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.18/ldb_toolchain_gen.sh)

5. 配置环境变量

6. 拉取 Doris 源码

   ```
   git clone http://github.com/apache/doris.git
   ```

7. 编译

   ```
   cd doris
   sh build.sh
   ```
## 注意事项

默认 WSL2 的发行版数据存储盘符为 C 盘，如有需要提前切换存储盘符，以防止系统盘符占满
