---
{
    "title": "编译",
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

    `$ docker pull apache/incubator-doris:build-env-ldb-toolchain-latest`
    
    检查镜像下载完成：
    
    ```
    $ docker images
    REPOSITORY              TAG                               IMAGE ID            CREATED             SIZE
    apache/incubator-doris  build-env-ldb-toolchain-latest    49f68cecbc1a        4 days ago          3.76GB
    ```

> 注1：针对不同的 Doris 版本，需要下载对应的镜像版本。从 Apache Doris 0.15 版本起，后续镜像版本号将与 Doris 版本号统一。比如可以使用 `apache/incubator-doris:build-env-for-0.15.0 `  来编译 0.15.0 版本。
>
> 注2：`apache/incubator-doris:build-env-ldb-toolchain-latest` 用于编译最新主干版本代码，会随主干版本不断更新。可以查看 `docker/README.md` 中的更新时间。

| 镜像版本 | commit id | doris 版本 |
|---|---|---|
| apache/incubator-doris:build-env | before [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) | 0.8.x, 0.9.x |
| apache/incubator-doris:build-env-1.1 | [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) | 0.10.x, 0.11.x |
| apache/incubator-doris:build-env-1.2 | [4ef5a8c](https://github.com/apache/incubator-doris/commit/4ef5a8c8560351d7fff7ff8fd51c4c7a75e006a8) | 0.12.x - 0.14.0 |
| apache/incubator-doris:build-env-1.3.1 | [ad67dd3](https://github.com/apache/incubator-doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) | 0.14.x |
| apache/incubator-doris:build-env-for-0.15.0 | [a81f4da](https://github.com/apache/incubator-doris/commit/a81f4da4e461a54782a96433b746d07be89e6b54) or later | 0.15.0 |
| apache/incubator-doris:build-env-latest | before [0efef1b](https://github.com/apache/incubator-doris/commit/0efef1b332300887ee0473f9df9bdd9d7297d824) | |
| apache/incubator-doris:build-env-ldb-toolchain-latest | trunk | |

**注意**：

> 1. 编译镜像 [ChangeLog](https://github.com/apache/incubator-doris/blob/master/thirdparty/CHANGELOG.md)。

> 2. doris 0.14.0 版本仍然使用apache/incubator-doris:build-env-1.2 编译，0.14.x 版本的代码将使用apache/incubator-doris:build-env-1.3.1。

> 3. 从 build-env-1.3.1 的docker镜像起，同时包含了 OpenJDK 8 和 OpenJDK 11，并且默认使用 OpenJDK 11 编译。请确保编译使用的 JDK 版本和运行时使用的 JDK 版本一致，否则会导致非预期的运行错误。你可以使用在进入编译镜像的容器后，使用以下命令切换默认 JDK 版本：
> 
>   切换到 JDK 8：
>   
>   ```
>   $ alternatives --set java java-1.8.0-openjdk.x86_64
>   $ alternatives --set javac java-1.8.0-openjdk.x86_64
>   $ export JAVA_HOME=/usr/lib/jvm/java-1.8.0
>   ```
>
>   切换到 JDK 11：
>   
>   ```
>   $ alternatives --set java java-11-openjdk.x86_64
>   $ alternatives --set javac java-11-openjdk.x86_64
>   $ export JAVA_HOME=/usr/lib/jvm/java-11
>   ```

2. 运行镜像

    `$ docker run -it apache/incubator-doris:build-env-ldb-toolchain-latest`
    
    建议以挂载本地 Doris 源码目录的方式运行镜像，这样编译的产出二进制文件会存储在宿主机中，不会因为镜像退出而消失。

    同时，建议同时将镜像中 maven 的 `.m2` 目录挂载到宿主机目录，以防止每次启动镜像编译时，重复下载 maven 的依赖库。

    ```
    $ docker run -it -v /your/local/.m2:/root/.m2 -v /your/local/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ apache/incubator-doris:build-env-ldb-toolchain-latest
    ```
    
3. 下载源码

    启动镜像后，你应该已经处于容器内。可以通过以下命令下载 Doris 源码（已挂载本地源码目录则不用）：
    
    ```
    $ wget https://dist.apache.org/repos/dist/dev/incubator/doris/xxx.tar.gz
    or
    $ git clone https://github.com/apache/incubator-doris.git
    ```

4. 编译 Doris

    ```
    $ sh build.sh
    ```
    
    >**注意:**
    >
    >如果你是第一次使用 `build-env-for-0.15.0` 或之后的版本，第一次编译的时候要使用如下命令：
    >
    > `sh build.sh --clean --be --fe --ui`
    >
    > 这是因为 build-env-for-0.15.0 版本镜像升级了 thrift(0.9 -> 0.13)，需要通过 --clean 命令强制使用新版本的 thrift 生成代码文件，否则会出现不兼容的代码。
    
    编译完成后，产出文件在 `output/` 目录中。

### 自行编译开发环境镜像

你也可以自己创建一个 Doris 开发环境镜像，具体可参阅 `docker/README.md` 文件。


## 直接编译（CentOS/Ubuntu）

你可以在自己的 linux 环境中直接尝试编译 Doris。

1. 系统依赖
   不同的版本依赖也不相同 
   * 在 [ad67dd3](https://github.com/apache/incubator-doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) 之前版本依赖如下：

      `GCC 7.3+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.11+     Bison 3.0+`
   
      如果使用Ubuntu 16.04 及以上系统 可以执行以下命令来安装依赖
   
      `sudo apt-get install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python autopoint pkg-config`
   
      如果是CentOS 可以执行以下命令
   
      `sudo yum groupinstall 'Development Tools' && sudo yum install maven cmake byacc flex automake libtool bison binutils-devel zip unzip ncurses-devel curl git wget python2 glibc-static libstdc++-static java-1.8.0-openjdk`

   * 在 [ad67dd3](https://github.com/apache/incubator-doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) 之后版本依赖如下：

      `GCC 10+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.19.2+ Bison 3.0+`

      如果使用Ubuntu 16.04 及以上系统 可以执行以下命令来安装依赖
      ```
      sudo apt install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python
      sudo add-apt-repository ppa:ubuntu-toolchain-r/ppa
      sudo apt update
      sudo apt install gcc-10 g++-10 
      sudo apt-get install autoconf automake libtool autopoint
      ```
      
      如果是CentOS 可以执行以下命令
      ```
      sudo yum groupinstall 'Development Tools' && sudo yum install maven cmake byacc flex automake libtool bison binutils-devel zip unzip ncurses-devel curl git wget python2 glibc-static libstdc++-static java-1.8.0-openjdk
      sudo yum install centos-release-scl
      sudo yum install devtoolset-10
      scl enable devtoolset-10 bash
      ```
      如果当前仓库没有提供devtoolset-10 可以添加如下repo 使用oracle 提供 package
      ```
      [ol7_software_collections]
      name=Software Collection packages for Oracle Linux 7 ($basearch)
      baseurl=http://yum.oracle.com/repo/OracleLinux/OL7/SoftwareCollections/$basearch/
      gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
      gpgcheck=1
      enabled=1
      ```
   
    安装完成后，自行设置环境变量 `PATH`, `JAVA_HOME` 等。(可以通过`alternatives --list`命令找到jdk的安装目录)
    注意： Doris 0.14.0 的版本仍然使用gcc7 的依赖编译，之后的代码将使用gcc10 的依赖

2. 编译 Doris

    ```
    $ sh build.sh
    ```
    
    编译完成后，产出文件在 `output/` 目录中。

## 常见问题

1. `Could not transfer artifact net.sourceforge.czt.dev:cup-maven-plugin:pom:1.6-cdh from/to xxx`

    如遇到上述错误，请参照 [PR #4769](https://github.com/apache/incubator-doris/pull/4769/files) 修改 `fe/pom.xml` 中 cloudera 相关的仓库配置。

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
