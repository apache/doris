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

    It is recommended to run the container by mounting the local Doris source directory, so that the compiled binary file will be stored in the host machine and will not disappear because the container exits.

     At the same time, it is recommended to mount the maven `.m2` directory in the mirror to the host directory at the same time to prevent repeated downloading of maven's dependent libraries each time the compilation is started.

    ```
    $ docker run -it -v /your/local/.m2:/root/.m2 -v /your/local/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ apachedoris/doris-dev:build-env
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

You can also create a Doris development environment mirror yourself, referring specifically to the `docker/README.md` file.


## Direct Compilation (CentOS/Ubuntu)

You can try to compile Doris directly in your own Linux environment.

1. System Dependence

    `GCC 7.3+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.11+ Bison 3.0+`

    If you are using Ubuntu 16.04 or newer, you can use the following command to install the dependencies

   `sudo apt-get install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python`
    
    If you are using CentOS you can use the following command to install the dependencies
   
   `sudo yum groupinstall 'Development Tools' && sudo yum install maven cmake byacc flex automake libtool bison binutils-devel zip unzip ncurses-devel curl git wget python2 glibc-static libstdc++-static java-1.8.0-openjdk`

    After installation, set environment variables `PATH`, `JAVA_HOME`, etc.

2. Compile Doris

    ```
    $ sh build.sh
    ```
	After compilation, the output file is in the `output/` directory.

## FAQ

1. `Could not transfer artifact net.sourceforge.czt.dev:cup-maven-plugin:pom:1.6-cdh from/to xxx`

    If you encounter the above error, please refer to [PR #4769](https://github.com/apache/incubator-doris/pull/4769/files) to modify the cloudera-related repo configuration in `fe/pom.xml`.
	
## Special statement

Starting from version 0.13, the dependency on the two third-party libraries [1] and [2] will be removed in the default compiled output. These two third-party libraries are under [GNU General Public License V3](https://www.gnu.org/licenses/gpl-3.0.en.html). This license is incompatible with [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0), so it should not appear in the Apache release by default.

Remove library [1] will result in the inability to access MySQL external tables. The feature of accessing MySQL external tables will be implemented through `UnixODBC` in future release version.

Remove library [2] will cause some data written in earlier versions (before version 0.8) to be unable to read. Because the data in the earlier version was compressed using the LZO algorithm, in later versions, it has been changed to the LZ4 compression algorithm. We will provide tools to detect and convert this part of the data in the future.

If required, users can continue to use these two dependent libraries. If you want to use it, you need to add the following options when compiling:

```
WITH_MYSQL=1 WITH_LZO=1 sh build.sh
```

Note that when users rely on these two third-party libraries, Doris is not used under the Apache License 2.0 by default. Please pay attention to the GPL related agreements.

* [1] mysql-5.7.18
* [2] lzo-2.10
