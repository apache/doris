---
{
    "title": "Compile on ARM platform",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->


# Compile and Run Doris on ARM64 + KylinOS.

This document describes how to compile Doris on the ARM64 platform.

Note that this document is only a guide document. Other errors may occur when compiling in different environments.

## Software and hardware environment

1. KylinOS version:

    ```
    $> cat /etc/.kyinfo
    name=Kylin-Server
    milestone=10-SP1-Release-Build04-20200711
    arch=arm64
    beta=False
    time=2020-07-11 17:16:54
    dist_id=Kylin-Server-10-SP1-Release-Build04-20200711-arm64-2020-07-11 17:16:54
    ```

2. CPU model

    ```
    $> cat /proc/cpuinfo
    model name: Phytium,FT-2000+/64
    ```

## Compile using ldb-toolchain

This method works with Doris versions after [commit 7f3564](https://github.com/apache/incubator-doris/commit/7f3564cca62de49c9f2ea67fcf735921dbebb4d1)

Download [ldbi\_toolchain\_gen.aarch64.sh](https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh)

For subsequent compilation, see [Compiling with LDB toolchain](./compilation-with-ldb-toolchain.md)

Note that both jdk and nodejs need to be downloaded with the corresponding aarch64 versions:

1. [Java8-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz)
2. [Node v12.13.0-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz)
