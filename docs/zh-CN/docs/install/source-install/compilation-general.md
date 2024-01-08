---
{
    "title": "通用编译",
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

# 编译

本文档主要介绍如何通过源码编译 Doris。

## 使用 Docker 开发镜像编译（推荐）

### 使用现成的镜像

1. 下载 Docker 镜像

   `$ docker pull apache/doris:build-env-ldb-toolchain-latest`

   检查镜像下载完成：

    ```
    $ docker images
    REPOSITORY              TAG                               IMAGE ID            CREATED             SIZE
    apache/doris  build-env-ldb-toolchain-latest    49f68cecbc1a        4 days ago          3.76GB
    ```

> 注1：针对不同的 Doris 版本，需要下载对应的镜像版本。从 Apache Doris 0.15 版本起，后续镜像版本号将与 Doris 版本号统一。比如可以使用 `apache/doris:build-env-for-0.15.0 `  来编译 0.15.0 版本。
>
> 注2：`apache/doris:build-env-ldb-toolchain-latest` 用于编译最新主干版本代码，会随主干版本不断更新。可以查看 `docker/README.md` 中的更新时间。

| 镜像版本 | commit id | doris 版本 |
|---|---|---|
| apache/incubator-doris:build-env | before [ff0dd0d](https://github.com/apache/doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) | 0.8.x, 0.9.x |
| apache/incubator-doris:build-env-1.1 | [ff0dd0d](https://github.com/apache/doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) | 0.10.x, 0.11.x |
| apache/incubator-doris:build-env-1.2 | [4ef5a8c](https://github.com/apache/doris/commit/4ef5a8c8560351d7fff7ff8fd51c4c7a75e006a8) | 0.12.x - 0.14.0 |
| apache/incubator-doris:build-env-1.3.1 | [ad67dd3](https://github.com/apache/doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) | 0.14.x |
| apache/doris:build-env-for-0.15.0 | [a81f4da](https://github.com/apache/doris/commit/a81f4da4e461a54782a96433b746d07be89e6b54) or later | 0.15.0 |
| apache/incubator-doris:build-env-latest | before [0efef1b](https://github.com/apache/doris/commit/0efef1b332300887ee0473f9df9bdd9d7297d824) | |
| apache/doris:build-env-for-1.0.0| | 1.0.0 |
| apache/doris:build-env-for-1.1.0| | 1.1.0 |
| apache/doris:build-env-for-1.2| | 1.1.x, 1.2.x |
| apache/doris:build-env-for-1.2-no-avx2| | 1.1.x, 1.2.x |
| apache/doris:build-env-for-2.0| | 2.0.x |
| apache/doris:build-env-for-2.0-no-avx2| | 2.0.x |
| apache/doris:build-env-ldb-toolchain-latest | | master |
| apache/doris:build-env-ldb-toolchain-no-avx2-latest | | master |

**注意**：

> 1. 名称中带有 no-avx2 字样的镜像中的第三方库，可以运行在不支持 avx2 指令的 CPU 上。可以配合 USE_AVX2=0 选项，编译 Doris。

> 2. 编译镜像 [ChangeLog](https://github.com/apache/doris/blob/master/thirdparty/CHANGELOG.md)。

> 3. doris 0.14.0 版本仍然使用apache/incubator-doris:build-env-1.2 编译，0.14.x 版本的代码将使用apache/incubator-doris:build-env-1.3.1。

> 4. 从 build-env-1.3.1 的docker镜像起，同时包含了 OpenJDK 8 和 OpenJDK 11，请通过 `java -version` 确认默认 JDK 版本。也可以通过以下方式切换版本（建议默认使用 JDK8）
    >
    >   切换到 JDK 8：
    >
    >   ```
    >   alternatives --set java java-1.8.0-openjdk.x86_64
    >   alternatives --set javac java-1.8.0-openjdk.x86_64
    >   export JAVA_HOME=/usr/lib/jvm/java-1.8.0
    >   ```
    >
    >   切换到 JDK 11：
    >
    >   ```
    >   alternatives --set java java-11-openjdk.x86_64
    >   alternatives --set javac java-11-openjdk.x86_64
    >   export JAVA_HOME=/usr/lib/jvm/java-11
    >   ```

2. 运行镜像

   `$ docker run -it apache/doris:build-env-ldb-toolchain-latest`

   建议以挂载本地 Doris 源码目录的方式运行镜像，这样编译的产出二进制文件会存储在宿主机中，不会因为镜像退出而消失。

   同时，建议同时将镜像中 maven 的 `.m2` 目录挂载到宿主机目录，以防止每次启动镜像编译时，重复下载 maven 的依赖库。
   
   此外，运行镜像编译时需要 download 部分文件，可以采用 host 模式启动镜像。 host 模式不需要加 -p 进行端口映射，因为和宿主机共享网络IP和端口。
   
   docker run 部分参数说明如下：
   
    | 参数 | 注释 |
    |---|---|
    | -v | 给容器挂载存储卷，挂载到容器的某个目录 |
    | --name | 指定容器名字，后续可以通过名字进行容器管理 |
    | --network | 容器网络设置: bridge 使用 docker daemon 指定的网桥，host 容器使用主机的网络， container:NAME_or_ID 使用其他容器的网路，共享IP和PORT等网络资源， none 容器使用自己的网络（类似--net=bridge），但是不进行配置 |
    
    如下示例，是指将容器的 /root/doris-DORIS-x.x.x-release 挂载至宿主机 /your/local/doris-DORIS-x.x.x-release 目录，且命名 mydocker 后用 host 模式 启动镜像：

    ```
    $ docker run -it --network=host --name mydocker -v /your/local/.m2:/root/.m2 -v /your/local/doris-DORIS-x.x.x-release/:/root/doris-DORIS-x.x.x-release/ apache/doris:build-env-ldb-toolchain-latest
    ```

3. 下载源码

   启动镜像后，你应该已经处于容器内。可以通过以下命令下载 Doris 源码（已挂载本地源码目录则不用）：

    ```
    $ git clone https://github.com/apache/doris.git
    ```

4. 编译 Doris

   先通过以下命令查看编译机器是否支持avx2指令集
   
    ```
   $ cat /proc/cpuinfo | grep avx2
    ```
   
   不支持则使用以下命令进行编译
   
   ```
   $ USE_AVX2=0  sh build.sh
   ```

   如果支持，可不加 USE_AVX2=0 ，直接进行编译
   
   ```
   $ sh build.sh
   ```

   如需编译Debug版本的BE，增加 BUILD_TYPE=Debug
   ```
   $ BUILD_TYPE=Debug sh build.sh
   ```

   编译完成后，产出文件在 `output/` 目录中。
   
   >**注意:**
   >
   >如果你是第一次使用 `build-env-for-0.15.0` 或之后的版本，第一次编译的时候要使用如下命令：
   >
   > `sh build.sh --clean --be --fe`
   >
   > 这是因为 build-env-for-0.15.0 版本镜像升级了 thrift(0.9 -> 0.13)，需要通过 --clean 命令强制使用新版本的 thrift 生成代码文件，否则会出现不兼容的代码。
   
   编译完成后，产出文件在 `output/` 目录中。

### 自行编译开发环境镜像

你也可以自己创建一个 Doris 开发环境镜像，具体可参阅 `docker/README.md` 文件。


## 直接编译（Ubuntu）

你可以在自己的 linux 环境中直接尝试编译 Doris。

1. 系统依赖
   不同的版本依赖也不相同
    * 在 [ad67dd3](https://github.com/apache/doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) 之前版本依赖如下：

      `GCC 7.3+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.11+     Bison 3.0+`

      如果使用Ubuntu 16.04 及以上系统 可以执行以下命令来安装依赖

      `sudo apt-get install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python autopoint pkg-config`

      如果是CentOS 可以执行以下命令

      `sudo yum groupinstall 'Development Tools' && sudo yum install maven cmake byacc flex automake libtool bison binutils-devel zip unzip ncurses-devel curl git wget python2 glibc-static libstdc++-static java-1.8.0-openjdk`

    * 在 [ad67dd3](https://github.com/apache/doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) 之后版本依赖如下：

      `GCC 10+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.19.2+ Bison 3.0+`

      如果使用Ubuntu 16.04 及以上系统 可以执行以下命令来安装依赖
       ```
       sudo apt install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python
       sudo add-apt-repository ppa:ubuntu-toolchain-r/ppa
       sudo apt update
       sudo apt install gcc-10 g++-10 
       sudo apt-get install autoconf automake libtool autopoint
       ```

2. 编译 Doris

    与使用 Docker 开发镜像编译一样，编译之前先检查是否支持avx2指令

    ```
   $ cat /proc/cpuinfo | grep avx2
    ```
    
    支持则使用下面命令进行编译

   ```
   $ sh build.sh
   ```
   
   如不支持需要加 USE_AVX2=0 
   
   ```
   $ USE_AVX2=0 sh build.sh
   ```

   如需编译Debug版本的BE，增加 BUILD_TYPE=Debug
   ```
   $ BUILD_TYPE=Debug sh build.sh
   ```

   编译完成后，产出文件在 `output/` 目录中。

## 常见问题

1. `Could not transfer artifact net.sourceforge.czt.dev:cup-maven-plugin:pom:1.6-cdh from/to xxx`

   如遇到上述错误，请参照 [PR #4769](https://github.com/apache/doris/pull/4769/files) 修改 `fe/pom.xml` 中 cloudera 相关的仓库配置。

2. 第三方依赖下载连接错误、失效等问题

   Doris 所依赖的第三方库的下载连接都在 `thirdparty/vars.sh` 文件内。随着时间推移，一些下载连接可能会失效。如果遇到这种情况。可以使用如下两种方式解决：

    1. 手动修改 `thirdparty/vars.sh` 文件

       手动修改有问题的下载连接和对应的 MD5 值。

    2. 使用第三方下载仓库：

        ```
        export REPOSITORY_URL=https://doris-thirdparty-repo.bj.bcebos.com/thirdparty
        sh build-thirdparty.sh
        ```

       REPOSITORY_URL 中包含所有第三方库源码包和他们的历史版本。

3. `fatal error: Killed signal terminated program ...`

   使用 Docker 镜像编译时如遇到上述报错，可能是分配给镜像的内存不足（Docker 默认分配的内存大小为 2GB，编译过程中内存占用的峰值大于 2GB）。

   尝试适当调大镜像的分配内存，推荐 4GB ~ 8GB。

4. 在使用Clang编译Doris时会默认使用PCH文件来加速编译过程，ccache的默认配置可能会导致PCH文件无法被缓存，或者缓存无法被命中，进而导致PCH被重复编译，拖慢编译速度，需要进行如下配置：  

   使用Clang编译，但不想使用PCH文件来加速编译过程，则需要加上参数`ENABLE_PCH=OFF`
   ```shell
   DORIS_TOOLCHAIN=clang ENABLE_PCH=OFF sh build.sh
   ```

## 特别声明

自 0.13 版本开始，默认的编译产出中将取消对 [1] 和 [2] 两个第三方库的依赖。这两个第三方库为 [GNU General Public License V3](https://www.gnu.org/licenses/gpl-3.0.en.html) 协议。该协议与 [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) 协议不兼容，因此默认不出现在 Apache 发布版本中。

移除依赖库 [1] 会导致无法访问 MySQL 外部表。访问 MySQL 外部表的功能会在后续版本中通过 UnixODBC 实现。

移除依赖库 [2] 会导致在无法读取部分早期版本（0.8版本之前）写入的部分数据。因为早期版本中的数据是使用 LZO 算法压缩的，在之后的版本中，已经更改为 LZ4 压缩算法。后续我们会提供工具用于检测和转换这部分数据。

如果有需求，用户可以继续使用这两个依赖库。如需使用，需要在编译时添加如下选项：

```
WITH_MYSQL=1 WITH_LZO=1 sh build.sh
```

注意，当用户依赖这两个第三方库时，则默认不在 Apache License 2.0 协议框架下使用 Doris。请注意 GPL 相关协议约束。

* [1] mysql-5.7.18
* [2] lzo-2.10
