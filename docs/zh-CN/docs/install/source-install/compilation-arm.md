---
{
    "title": "在 Arm 平台上编译",
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

# Apache Doris Arm 架构编译

本文档介绍如何在 ARM64 平台上编译 Doris。

注意，该文档仅作为指导性文档。在不同环境中编译可能出现其他错误。

## KylinOS

### 软硬件环境

1. KylinOS 版本：

    ```
    $> cat /etc/.kyinfo
    name=Kylin-Server
    milestone=10-SP1-Release-Build04-20200711
    arch=arm64
    beta=False
    time=2020-07-11 17:16:54
    dist_id=Kylin-Server-10-SP1-Release-Build04-20200711-arm64-2020-07-11 17:16:54
    ```

2. CPU型号：

    ```
    $> cat /proc/cpuinfo
    model name  : Phytium,FT-2000+/64
    ```

### 使用 ldb-toolchain 编译

该方法适用于 [commit 7f3564](https://github.com/apache/doris/commit/7f3564cca62de49c9f2ea67fcf735921dbebb4d1) 之后的 Doris 版本。

下载 [ldb\_toolchain\_gen.aarch64.sh](https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh)

之后的编译方式参阅 [使用 LDB toolchain 编译](./compilation-with-ldb-toolchain.md)

注意其中 jdk 和 nodejs 都需要下载对应的 aarch64 版本：

1. [Java8-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz)
2. [Node v12.13.0-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz)

## CentOS & Ubuntu

### 硬件环境

1. 系统版本：CentOS 8.4、Ubuntu 20.04
2. 系统架构：ARM X64
3. CPU：4 C
4. 内存：16 GB
5. 硬盘：40GB（SSD）、100GB（SSD）

### 软件环境

#### 软件环境对照表

| 组件名称                                                     | 组件版本             |
| ------------------------------------------------------------ | -------------------- |
| Git                                                          | 2.0+                 |
| JDK                                                          | 1.8.0                |
| Maven                                                        | 3.6.3                |
| NodeJS                                                       | 16.3.0               |
| LDB-Toolchain                                                | 0.9.1                |
| 常备环境：<br />byacc<br />patch<br />automake<br />libtool<br />make<br />which<br />file<br />ncurses-devel<br />gettext-devel<br />unzip<br />bzip2<br />zip<br />util-linux<br />wget<br />git<br />python2 | yum或apt自动安装即可 |
| autoconf                                                     | 2.69                 |
| bison                                                        | 3.0.4                |

#### 软件环境安装命令

##### CentOS 8.4

- 创建软件下载安装包根目录和软件安装根目录

  ```shell
  # 创建软件下载安装包根目录
  mkdir /opt/tools
  # 创建软件安装根目录
  mkdir /opt/software
  ```

- Git

  ```shell
  # 省去编译麻烦，直接使用 yum 安装
  yum install -y git
  ```

- JDK8

  ```shell
  # 两种方式，第一种是省去额外下载和配置，直接使用 yum 安装，安装 devel 包是为了获取一些工具，如 jps 命令
  yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
  
  # 第二种是下载 arm64 架构的安装包，解压配置环境变量后使用
  cd /opt/tools
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz && \
  	tar -zxvf jdk-8u291-linux-aarch64.tar.gz && \
  	mv jdk1.8.0_291 /opt/software/jdk8
  ```

- Maven

  ```shell
  cd /opt/tools
  # wget 工具下载后，直接解压缩配置环境变量使用
  wget https://dlcdn.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz && \
  	tar -zxvf apache-maven-3.6.3-bin.tar.gz && \
  	mv apache-maven-3.6.3 /opt/software/maven
  ```

- NodeJS

  ```shell
  cd /opt/tools
  # 下载 arm64 架构的安装包
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz && \
  	tar -xvf node-v16.3.0-linux-arm64.tar.xz && \
  	mv node-v16.3.0-linux-arm64 /opt/software/nodejs
  ```

- LDB-Toolchain

  ```shell
  cd /opt/tools
  # 下载 LDB-Toolchain ARM 版本
  wget https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh && \
  	sh ldb_toolchain_gen.aarch64.sh /opt/software/ldb_toolchain/
  ```

- 配置环境变量

  ```shell
  # 配置环境变量
  vim /etc/profile.d/doris.sh
  export JAVA_HOME=/opt/software/jdk8
  export MAVEN_HOME=/opt/software/maven
  export NODE_JS_HOME=/opt/software/nodejs
  export LDB_HOME=/opt/software/ldb_toolchain
  export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$NODE_JS_HOME/bin:$LDB_HOME/bin:$PATH
  
  # 保存退出并刷新环境变量
  source /etc/profile.d/doris.sh
  
  # 测试是否成功
  java -version
  > java version "1.8.0_291"
  mvn -version
  > Apache Maven 3.6.3
  node --version
  > v16.3.0
  gcc --version
  > gcc-11
  ```

- 安装其他额外环境和组件

  ```shell
  # install required system packages
  sudo yum install -y byacc patch automake libtool make which file ncurses-devel gettext-devel unzip bzip2 bison zip util-linux wget git python2
  
  # install autoconf-2.69
  cd /opt/tools
  wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz && \
      tar zxf autoconf-2.69.tar.gz && \
      mv autoconf-2.69 /opt/software/autoconf && \
      cd /opt/software/autoconf && \
      ./configure && \
      make && \
      make install
  ```

##### Ubuntu 20.04

- 更新 apt-get 软件库

  ```shell
  apt-get update
  ```

- 检查 shell 命令集

  ubuntu 的 shell 默认安装的是 dash，而不是 bash，要切换成 bash 才能执行，运行以下命令查看 sh 的详细信息，确认 shell 对应的程序是哪个：

  ```shell
  ls -al /bin/sh
  ```

  通过以下方式可以使 shell 切换回 bash：

  ```shell
  sudo dpkg-reconfigure dash
  ```

  然后选择 no 或者 否 ，并确认

  这样做将重新配置 dash，并使其不作为默认的 shell 工具

- 创建软件下载安装包根目录和软件安装根目录

  ```shell
  # 创建软件下载安装包根目录
  mkdir /opt/tools
  # 创建软件安装根目录
  mkdir /opt/software
  ```

- Git

  ```shell
  # 省去编译麻烦，直接使用 apt-get 安装
  apt-get -y install git
  ```

- JDK8

  ```shell
  # 下载 arm64 架构的安装包，解压配置环境变量后使用
  cd /opt/tools
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz && \
  	tar -zxvf jdk-8u291-linux-aarch64.tar.gz && \
  	mv jdk1.8.0_291 /opt/software/jdk8
  ```

- Maven

  ```shell
  cd /opt/tools
  # wget 工具下载后，直接解压缩配置环境变量使用
  wget https://dlcdn.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz && \
  	tar -zxvf apache-maven-3.6.3-bin.tar.gz && \
  	mv apache-maven-3.6.3 /opt/software/maven
  ```

- NodeJS

  ```shell
  cd /opt/tools
  # 下载 arm64 架构的安装包
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz && \
  	tar -xvf node-v16.3.0-linux-arm64.tar.xz && \
  	mv node-v16.3.0-linux-arm64 /opt/software/nodejs
  ```

- LDB-Toolchain

  ```shell
  cd /opt/tools
  # 下载 LDB-Toolchain ARM 版本
  wget https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh && \
  sh ldb_toolchain_gen.aarch64.sh /opt/software/ldb_toolchain/
  ```

- 配置环境变量

  ```shell
  # 配置环境变量
  vim /etc/profile.d/doris.sh
  export JAVA_HOME=/opt/software/jdk8
  export MAVEN_HOME=/opt/software/maven
  export NODE_JS_HOME=/opt/software/nodejs
  export LDB_HOME=/opt/software/ldb_toolchain
  export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$NODE_JS_HOME/bin:$LDB_HOME/bin:$PATH
  
  # 保存退出并刷新环境变量
  source /etc/profile.d/doris.sh
  
  # 测试是否成功
  java -version
  > java version "1.8.0_291"
  mvn -version
  > Apache Maven 3.6.3
  node --version
  > v16.3.0
  gcc --version
  > gcc-11
  ```

- 安装其他额外环境和组件

  ```shell
  # install required system packages
  sudo apt install -y build-essential cmake flex automake bison binutils-dev libiberty-dev zip libncurses5-dev curl ninja-build
  sudo apt-get install -y make
  sudo apt-get install -y unzip
  sudo apt-get install -y python2
  sudo apt-get install -y byacc
  sudo apt-get install -y automake
  sudo apt-get install -y libtool
  sudo apt-get install -y bzip2
  sudo add-apt-repository ppa:ubuntu-toolchain-r/ppa 
  sudo apt update
  sudo apt install gcc-11 g++-11 
  sudo apt-get -y install autoconf autopoint
  
  # install autoconf-2.69
  cd /opt/tools
  wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz && \
      tar zxf autoconf-2.69.tar.gz && \
      mv autoconf-2.69 /opt/software/autoconf && \
      cd /opt/software/autoconf && \
      ./configure && \
      make && \
      make install
  ```

#### 下载源码

```shell
cd /opt
git clone https://github.com/apache/doris.git
```

#### 安装部署

##### 查看是否支持 AVX2 指令集

若有数据返回，则代表支持，若无数据返回，则代表不支持

```shell
cat /proc/cpuinfo | grep avx2
```

##### 执行编译

```shell
# 支持 AVX2 指令集的机器，直接编译即可
sh build.sh
# 不支持 AVX2 指令集的机器，使用如下命令编译
USE_AVX2=OFF sh build.sh
```

### 常见问题

1. 编译第三方库 libhdfs3.a ，找不到文件夹

   - 问题描述

     在执行编译安装过程中，出现了如下报错

     > not found lib/libhdfs3.a file or directory

   - 问题原因

     第三方库的依赖下载有问题

   - 解决方案

     - 使用第三方下载仓库

       ```shell
       export REPOSITORY_URL=https://doris-thirdparty-repo.bj.bcebos.com/thirdparty
       sh /opt/doris/thirdparty/build-thirdparty.sh
       ```

       REPOSITORY_URL 中包含所有第三方库源码包和他们的历史版本。

2. python 命令未找到

   - 问题描述

     - 执行 build.sh 时抛出异常

       > /opt/doris/env.sh: line 46: python: command not found
       >
       > Python 2.7.18

   - 问题原因

     经查找，发现该系统默认使用 `python2.7`、 `python3.6`、`python2`、`python3` 这几个命令来执行 python 命令，Doris安装依赖需要 python 2.7+ 版本即可，故只需要添加名为 `python` 的命令连接即可，使用版本2和版本3的都可以

   - 解决方案

     建立 `\usr\bin` 中 `python` 命令的软连接

     ```shell
     # 查看python安装目录
     whereis python
     # 建立软连接
     sudo ln -s /usr/bin/python2.7 /usr/bin/python
     ```

3. 编译结束后没有 output 目录

   - 问题描述

     - build.sh 执行结束后，目录中未发现 output 文件夹.

   - 问题原因

     未成功编译，需重新编译

   - 解决方案

     ```shell
     sh build.sh --clean
     ```

4. spark-dpp 编译失败

   - 问题描述

     - 执行 build.sh 编译以后，编译至 Spark-DPP 报错失败

       > Failed to execute goal on project spark-dpp

   - 问题原因

     最后的错误提示，是由于下载失败（且由于是未能连接到 repo.maven.apache.org 中央仓库）的问题

     > Could not transfer artifact org.apache.spark:spark-sql_2.12:jar:2.4.6 from/to central (https://repo.maven.apache.org/maven2): Transfer failed for https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql_2.12/2.4.6/spark-sql_2.12-2.4.6.jar: Unknown host repo.maven.apache.org

     重新 build

   - 解决方案

     - 重新 build

5. 剩余空间不足，编译失败

   - 问题描述

     - 编译过程中报 构建 CXX 对象失败，提示剩余空间不足

       >  fatal error: error writing to /tmp/ccKn4nPK.s: No space left on device
       >  1112 | } // namespace doris::vectorized
       >  compilation terminated.

   - 问题原因

     设备剩余空间不足

   - 解决方案

     扩大设备剩余空间，如删除不需要的文件等

6. 启动FE失败，事务-20 问题

   - 问题描述

     在启动 FE 时，报事务错误 20 问题，状态为 UNKNOWN

     > [BDBEnvironment.setup():198] error to open replicated environment. will exit.
     > com.sleepycat.je.rep.ReplicaWriteException: (JE 18.3.12) Problem closing transaction 20. The current state is:UNKNOWN. The node transitioned to this state at:Fri Apr 22 12:48:08 CST 2022

   - 问题原因

     硬盘空间不足，需更多空间

   - 解决方案

     释放硬盘空间或者挂载新硬盘

7. BDB 环境设置异常，磁盘寻找错误

   - 问题描述

     在迁移 FE 所在的盘符后启动 FE 报异常

     > 2022-04-22 16:21:44,092 ERROR (MASTER 172.28.7.231_9010_1650606822109(-1)|1) [BDBJEJournal.open():306] catch an exception when setup bdb environment. will exit.
     > com.sleepycat.je.DiskLimitException: (JE 18.3.12) Disk usage is not within je.maxDisk or je.freeDisk limits and write operations are prohibited: maxDiskLimit=0 freeDiskLimit=5,368,709,120 adjustedMaxDiskLimit=0 maxDiskOverage=0 freeDiskShortage=1,536,552,960 diskFreeSpace=3,832,156,160 availableLogSize=-1,536,552,960 totalLogSize=4,665 activeLogSize=4,665 reservedLogSize=0 protectedLogSize=0 protectedLogSizeMap={}

   - 问题原因

     迁移了 FE 所在的位置，元数据存储的硬盘信息无法匹配到，或者该硬盘损坏或未挂载

   - 解决方案

     - 检查硬盘是否正常，是否初始化并正确挂载
     - 修复 FE 元数据
     - 若为测试机器，则可以删除元数据目录重新启动

8. 在 pkg.config 中找不到 pkg.m4 文件

   - 问题描述

     - 编译过程中出现了找不到文件错误，报错如下

       > Couldn't find pkg.m4 from pkg-config. Install the appropriate package for your distribution or set ACLOCAL_PATH to the directory containing pkg.m4.

     - 通过查找上面的日志，发现是 `libxml2` 这个三方库在编译的时候出现了问题

   - 问题原因

     `libxml2` 三方库编译错误，找不到 pkg.m4 文件

     ***猜测：***

     1. Ubuntu 系统加载环境变量时有异常，导致 ldb 目录下的索引未被成功加载
     2. 在 libxml2 编译时检索环境变量失效，导致编译过程没有检索到 ldb/aclocal 目录

   - 解决方案

     将 ldb/aclocal 目录下的 `pkg.m4` 文件拷贝至 libxml2/m4 目录下，重新编译第三方库

     ```shell
     cp /opt/software/ldb_toolchain/share/aclocal/pkg.m4 /opt/doris/thirdparty/src/libxml2-v2.9.10/m4
     sh /opt/doris/thirdparty/build-thirdparty.sh
     ```

9. 执行测试 CURL_HAS_TLS_PROXY 失败

   - 问题描述

     - 三方包编译过程报错，错误如下

       > -- Performing Test CURL_HAS_TLS_PROXY - Failed
       > CMake Error at cmake/dependencies.cmake:15 (get_property):
       > INTERFACE_LIBRARY targets may only have whitelisted properties.  The
       > property "LINK_LIBRARIES_ALL" is not allowed.

     - 查看日志以后，发现内部是由于 curl `No such file or directory`

       > fatal error: curl/curl.h: No such file or directory
       >  2 |     #include <curl/curl.h>
       > compilation terminated.
       > ninja: build stopped: subcommand failed.

   - 问题原因

     编译环境有错误，查看 gcc 版本后发现是系统自带的 9.3.0 版本，故而没有走 ldb 编译，需设置 ldb 环境变量

   - 解决方案

     配置 ldb 环境变量

     ```shell
     # 配置环境变量
     vim /etc/profile.d/ldb.sh
     export LDB_HOME=/opt/software/ldb_toolchain
     export PATH=$LDB_HOME/bin:$PATH
     # 保存退出并刷新环境变量
     source /etc/profile.d/ldb.sh
     # 测试
     gcc --version
     > gcc-11
     ```

10. 其他异常问题

   - 问题描述

     如有以下组件的错误提示，则统一以该方案解决

     - bison 相关
       1. 安装 bison-3.0.4 时报 fseterr.c 错误
     - flex 相关
       1. flex 命令未找到
     - cmake 相关
       1. cmake 命令未找到
       2. cmake 找不到依赖库
       3. cmake 找不到 CMAKE_ROOT
       4. cmake 环境变量 CXX 中找不到编译器集
     - boost 相关
       1. Boost.Build 构建引擎失败
     - mysql 相关
       1. 找不到 mysql 的客户端依赖 a 文件
     - gcc 相关
       1. GCC 版本需要11+

   - 问题原因

     未使用 Ldb-Toolschain 进行编译

   - 解决方案

     - 检查 Ldb-Toolschain 环境变量是否配置
     - 查看 gcc 版本是否是 `gcc-11`
     - 删除 `ldb-toolschain.sh` 脚本执行后的 ldb 目录，重新执行并配置环境变量，验证 gcc 版本

