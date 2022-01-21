---
{
    "title": "使用 LDB toolchain 编译",
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

# 使用 LDB toolchain 编译

本文档主要介绍如何使用 LDB toolchain 编译 Doris。该方式后续会替换 Docker 开发镜像成为 Doris 默认的源码编译方式。

> 感谢 [Amos Bird](https://github.com/amosbird) 的贡献。

## 准备编译环境

该方式适用于绝大多数 Linux 发行版（CentOS，Ubuntu 等）。

1. 下载 `ldb_toolchain_gen.sh`

    可以从 [这里](https://github.com/amosbird/ldb_toolchain_gen/releases) 下载最新的 `ldb_toolchain_gen.sh`。该脚本用于生成 ldb toolchain。
    
    > 更多信息，可访问 [https://github.com/amosbird/ldb_toolchain_gen](https://github.com/amosbird/ldb_toolchain_gen)

2. 执行以下命令生成 ldb toolchain

    ```
    sh ldb_toolchain_gen.sh /path/to/ldb_toolchain/
    ```
    
    其中 `/path/to/ldb_toolchain/` 为安装 toolchain 目录。
    
    执行成功后，会在 `/path/to/ldb_toolchain/` 下生成如下目录结构：
    
    ```
    ├── bin
    ├── include
    ├── lib
    ├── share
    ├── test
    └── usr
    ```
    
3. 下载并安装其他编译组件

    1. [Java8](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u131-linux-x64.tar.gz)
    2. [Apache Maven 3.8.4](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/apache-maven-3.6.3-bin.tar.gz)
    3. [Node v12.13.0](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v12.13.0-linux-x64.tar.gz)

    同时需要自行安装 python。

4. 下载 Doris 源码

    ```
    git clone https://github.com/apache/incubator-doris.git
    ```
    
    下载完成后，进入到 doris 源码目录，创建 `custom_env.sh`，文件，并设置 PATH 环境变量，如：
    
    ```
    export JAVA_HOME=/path/to/jave/
    export PATH=$JAVA_HOME/bin:$PATH
    export PATH=/path/to/maven/bin:$PATH
    export PATH=/path/to/node/bin:$PATH
    export PATH=/path/to/ldb-toolchain/bin:$PATH
    ```

## 编译 Doris

进入 Doris 源码目录，执行：

```
sh build.sh
```

该脚本会先编译第三方库，之后再编译 Doris 组件（FE、BE）。编译产出在 `output/` 目录下。
