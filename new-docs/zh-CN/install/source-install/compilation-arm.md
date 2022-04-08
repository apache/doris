---
{
    "title": "在ARM平台上编译",
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

# ARM64 + KylinOS 编译运行 Doris

本文档介绍如何在 ARM64 平台上编译 Doris。

注意，该文档仅作为指导性文档。在不同环境中编译可能出现其他错误。

## 软硬件环境

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

2. CPU型号

    ```
    $> cat /proc/cpuinfo
    model name  : Phytium,FT-2000+/64
    ```

## 使用 ldb-toolchain 编译

该方法适用于 [commit 7f3564](https://github.com/apache/incubator-doris/commit/7f3564cca62de49c9f2ea67fcf735921dbebb4d1) 之后的 Doris 版本。

下载 [ldb\_toolchain\_gen.aarch64.sh](https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh)

之后的编译方式参阅 [使用 LDB toolchain 编译](./compilation-with-ldb-toolchain.md)

注意其中 jdk 和 nodejs 都需要下载对应的 aarch64 版本：

1. [Java8-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz)
2. [Node v12.13.0-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz)
