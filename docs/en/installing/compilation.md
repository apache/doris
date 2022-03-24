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

    `$ docker pull apache/incubator-doris:build-env-ldb-toolchain-latest`

    Check mirror download completed:

    ```
    $ docker images
    REPOSITORY               TAG                              IMAGE ID            CREATED             SIZE
    apache/incubator-doris   build-env-ldb-toolchain-latest   49f68cecbc1a        4 days ago          3.76GB
    ```

> Note1: For different versions of Doris, you need to download the corresponding mirror version. From Apache Doris 0.15 version, the docker image will keep same version number with Doris. For example, you can use  `apache/incubator-doris:build-env-for-0.15.0` to compile Apache Doris 0.15.0.
>
> Node2: `apache/incubator-doris:build-env-ldb-toolchain-latest` is for compiling trunk code, and will be updated along with trunk code. View the update time in `docker/README.md`

| image version | commit id | release version |
|---|---|---|
| apache/incubator-doris:build-env | before [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) | 0.8.x, 0.9.x |
| apache/incubator-doris:build-env-1.1 | [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) or later | 0.10.x or later |
| apache/incubator-doris:build-env-1.2 | [4ef5a8c](https://github.com/apache/incubator-doris/commit/4ef5a8c8560351d7fff7ff8fd51c4c7a75e006a8) or later | 0.12.x - 0.14.0 |
| apache/incubator-doris:build-env-1.3.1 | [ad67dd3](https://github.com/apache/incubator-doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) or later | 0.14.x |
| apache/incubator-doris:build-env-for-0.15.0 | [a81f4da](https://github.com/apache/incubator-doris/commit/a81f4da4e461a54782a96433b746d07be89e6b54) or later | 0.15.0          |
| apache/incubator-doris:build-env-latest | before [0efef1b](https://github.com/apache/incubator-doris/commit/0efef1b332300887ee0473f9df9bdd9d7297d824) | |
| apache/incubator-doris:build-env-ldb-toolchain-latest | trunk | trunk |

**note**:

> 1. Dev docker image [ChangeLog](https://github.com/apache/incubator-doris/blob/master/thirdparty/CHANGELOG.md)

> 2. Doris version 0.14.0 still uses apache/incubator-doris:build-env-1.2 to compile, and the 0.14.x code will use apache/incubator-doris:build-env-1.3.1.

> 3. From docker image of build-env-1.3.1, both OpenJDK 8 and OpenJDK 11 are included, and OpenJDK 11 is used for compilation by default. Please make sure that the JDK version used for compiling is the same as the JDK version used at runtime, otherwise it may cause unexpected operation errors. You can use the following command to switch the default JDK version in container:
>
>   Switch to JDK 8:
>
>   ```
>   $ alternatives --set java java-1.8.0-openjdk.x86_64
>   $ alternatives --set javac java-1.8.0-openjdk.x86_64
>   $ export JAVA_HOME=/usr/lib/jvm/java-1.8.0
>   ```
>
>   Switch to JDK 11:
>
>   ```
>   $ alternatives --set java java-11-openjdk.x86_64
>   $ alternatives --set javac java-11-openjdk.x86_64
>   $ export JAVA_HOME=/usr/lib/jvm/java-11
>   ```

2. Running Mirror

    `$ docker run -it apache/incubator-doris:build-env-ldb-toolchain-latest`

    It is recommended to run the container by mounting the local Doris source directory, so that the compiled binary file will be stored in the host machine and will not disappear because the container exits.

     At the same time, it is recommended to mount the maven `.m2` directory in the mirror to the host directory at the same time to prevent repeated downloading of maven's dependent libraries each time the compilation is started.

    ```
    $ docker run -it -v /your/local/.m2:/root/.m2 -v /your/local/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ apache/incubator-doris:build-env-ldb-toolchain-latest
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

    > ** Note: **
     >
     > If you are using `build-env-for-0.15.0` or later version for the first time, use the following command when compiling:
     >
     > `sh build.sh --clean --be --fe --ui`
     >
     > This is because from build-env-for-0.15.0, we upgraded thrift (0.9 -> 0.13), you need to use the --clean command to force the use of the new version of thrift to generate code files, otherwise incompatible code will appear.

    After compilation, the output file is in the `output/` directory.

### Self-compiling Development Environment Mirror

You can also create a Doris development environment mirror yourself, referring specifically to the `docker/README.md` file.


## Direct Compilation (CentOS/Ubuntu)

You can try to compile Doris directly in your own Linux environment.

1. System Dependence
    * Before commit [ad67dd3](https://github.com/apache/incubator-doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) will use the dependencies as follows:

       `GCC 7.3+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.11+ Bison 3.0+`

       If you are using Ubuntu 16.04 or newer, you can use the following command to install the dependencies

       `sudo apt-get install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python autopoint pkg-config`

       If you are using CentOS you can use the following command to install the dependencies

       `sudo yum groupinstall 'Development Tools' && sudo yum install maven cmake byacc flex automake libtool bison binutils-devel zip unzip ncurses-devel curl git wget python2 glibc-static libstdc++-static java-1.8.0-openjdk`

    * After commit [ad67dd3](https://github.com/apache/incubator-doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) will use the dependencies as follows:

       `GCC 10+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.19.2+ Bison 3.0+`

       If you are using Ubuntu 16.04 or newer, you can use the following command to install the dependencies

       ```
       sudo apt install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python
       sudo add-apt-repository ppa:ubuntu-toolchain-r/ppa
       sudo apt update
       sudo apt install gcc-10 g++-10
       sudo apt-get install autoconf automake libtool autopoint
       ```
        If you are using CentOS you can use the following command to install the dependencies

       ```
       sudo yum groupinstall 'Development Tools' && sudo yum install maven cmake byacc flex automake libtool bison binutils-devel zip unzip ncurses-devel curl git wget python2 glibc-static libstdc++-static java-1.8.0-openjdk
       sudo yum install centos-release-scl
       sudo yum install devtoolset-10
       scl enable devtoolset-10 bash
       ```
       If devtoolset-10 is not found in current repo. Oracle has already rebuilt the devtoolset-10 packages. You can use this repo file:
       ```
       [ol7_software_collections]
       name=Software Collection packages for Oracle Linux 7 ($basearch)
       baseurl=http://yum.oracle.com/repo/OracleLinux/OL7/SoftwareCollections/$basearch/
       gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-oracle
       gpgcheck=1
       enabled=1
       ```
       After installation, set environment variables `PATH`, `JAVA_HOME`, etc.
       > nit: you can find the jdk install directory by using command `alternatives --list`

       Doris 0.14.0 will use gcc7 env to compile.

2. Compile Doris

    ```
    $ sh build.sh
    ```
    After compilation, the output file is in the `output/` directory.

## FAQ

1. `Could not transfer artifact net.sourceforge.czt.dev:cup-maven-plugin:pom:1.6-cdh from/to xxx`

    If you encounter the above error, please refer to [PR #4769](https://github.com/apache/incubator-doris/pull/4769/files) to modify the cloudera-related repo configuration in `fe/pom.xml`.

2. The third party relies on download connection errors, failures, etc.

     The download links of the third-party libraries that Doris relies on are all in the `thirdparty/vars.sh` file. Over time, some download connections may fail. If you encounter this situation. It can be solved in the following two ways:

     1. Manually modify the `thirdparty/vars.sh` file

         Manually modify the problematic download connection and the corresponding MD5 value.

     2. Use a third-party download warehouse:

         ```
         export REPOSITORY_URL=https://doris-thirdparty-repo.bj.bcebos.com/thirdparty
         sh build-thirdparty.sh
         ```

         REPOSITORY_URL contains all third-party library source code packages and their historical versions.

3. `fatal error: Killed signal terminated program ...`

     If you encounter the above error when compiling with a Docker image, it may be that the memory allocated to the image is insufficient (the default memory size allocated by Docker is 2GB, and the peak memory usage during the compilation process is greater than 2GB).

     Try to increase the allocated memory of the image appropriately, 4GB ~ 8GB is recommended.

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
