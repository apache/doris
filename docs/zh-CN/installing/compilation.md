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

    `$ docker pull apachedoris/doris-dev:build-env`
    
    检查镜像下载完成：
    
    ```
    $ docker images
    REPOSITORY              TAG                 IMAGE ID            CREATED             SIZE
    apachedoris/doris-dev   build-env           f8bc5d4024e0        21 hours ago        3.28GB
    ```
    
注: 针对不同的 Doris 版本，需要下载对应的镜像版本

| image version | commit id | release version |
|---|---|---|
| apachedoris/doris-dev:build-env | before [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) | 0.8.x, 0.9.x |
| apachedoris/doris-dev:build-env-1.1 | [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) [4ef5a8c](https://github.com/apache/incubator-doris/commit/4ef5a8c8560351d7fff7ff8fd51c4c7a75e006a8) | 0.10.x, 0.11.x |
| apachedoris/doris-dev:build-env-1.2 | [4ef5a8c](https://github.com/apache/incubator-doris/commit/4ef5a8c8560351d7fff7ff8fd51c4c7a75e006a8) or later | 0.12.x or later

2. 运行镜像

    `$ docker run -it apachedoris/doris-dev:build-env`
    
    建议以挂载本地 Doris 源码目录的方式运行镜像，这样编译的产出二进制文件会存储在宿主机中，不会因为镜像退出而消失。

    同时，建议同时将镜像中 maven 的 `.m2` 目录挂载到宿主机目录，以防止每次启动镜像编译时，重复下载 maven 的依赖库。

    ```
    $ docker run -it -v /your/local/.m2:/root/.m2 -v /your/local/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ apachedoris/doris-dev:build-env
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
    
    编译完成后，产出文件在 `output/` 目录中。
    
### 自行编译开发环境镜像

你也可以自己创建一个 Doris 开发环境镜像，具体可参阅 `docker/README.md` 文件。


## 直接编译（CentOS/Ubuntu）

你可以在自己的 linux 环境中直接尝试编译 Doris。

1. 系统依赖

    `GCC 7.3+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.11+     Bison 3.0+`

    如果使用Ubuntu 16.04 及以上系统 可以执行以下命令来安装依赖
    
    `sudo apt-get install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python`

    如果是CentOS 可以执行以下命令

    `sudo yum groupinstall 'Development Tools' && sudo yum install maven cmake byacc flex automake libtool bison binutils-devel zip unzip ncurses-devel curl git wget python2 glibc-static libstdc++-static java-1.8.0-openjdk`

    安装完成后，自行设置环境变量 `PATH`, `JAVA_HOME` 等。
    
2. 编译 Doris

    ```
    $ sh build.sh
    ```
    
    编译完成后，产出文件在 `output/` 目录中。

## 常见问题

1. `Could not transfer artifact net.sourceforge.czt.dev:cup-maven-plugin:pom:1.6-cdh from/to xxx`

    如遇到上述错误，请参照 [PR #4769](https://github.com/apache/incubator-doris/pull/4769/files) 修改 `fe/pom.xml` 中 cloudera 相关的仓库配置。

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
