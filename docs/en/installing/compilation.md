---
{
    "title": "Compilation",
    "language": "en"
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


# Compilation

This document focuses on how to code Doris through source code.

## Developing mirror compilation using Docker (recommended)

### Use off-the-shelf mirrors

1. Download Docker Mirror

	`$ docker pull apachedoris/doris-dev:build-env`

	Check mirror download completed:

    ```
    $ docker images
    REPOSITORY              TAG                 IMAGE ID            CREATED             SIZE
    apachedoris/doris-dev   build-env           f8bc5d4024e0        21 hours ago        3.28GB
    ```

Note: For different versions of Oris, you need to download the corresponding mirror version.

| image version | commit id | release version |
|---|---|---|
| apachedoris/doris-dev:build-env | before [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) | 0.8.x, 0.9.x |
| apachedoris/doris-dev:build-env-1.1 | [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) or later | 0.10.x or later |

2. Running Mirror

	`$ docker run -it apachedoris/doris-dev:build-env`

	If you want to compile the local Doris source code, you can mount the path:

    ```
    $ docker run -it -v /your/local/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ apachedoris/doris-dev:build-env
    ```

3. Download source code

	After starting the mirror, you should be in the container. The Doris source code can be downloaded from the following command (local source directory mounted is not required):

    ```
    $ wget https://dist.apache.org/repos/dist/dev/incubator/doris/xxx.tar.gz
    or
    $ git clone https://github.com/apache/incubator-doris.git
    ```

4. Compile Doris

    ```
    $ sh build.sh
    ```

	After compilation, the output file is in the `output/` directory.

### Self-compiling Development Environment Mirror

You can also create a Doris development environment mirror yourself, referring specifically to the `docker/README.md'file.


## Direct Compilation (CentOS/Ubuntu)

You can try to compile Doris directly in your own Linux environment.

1. System Dependence

    `GCC 5.3.1+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.11+`

    If you are using Ubuntu 16.04 or newer, you can use the following command to install the dependencies
    
    `sudo apt-get install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev`

    After installation, set environment variables `PATH`, `JAVA_HOME`, etc.

2. Compile Doris

    ```
    $ sh build.sh
    ```
	After compilation, the output file is in the `output/` directory.
