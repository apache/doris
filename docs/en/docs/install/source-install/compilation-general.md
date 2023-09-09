---
{
    "title": "General Compilation",
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

This topic is about how to compile Doris from source.

## Compile with Docker Development Image (Recommended)

### Use Off-the-Shelf Image

1. Download Docker image

    `$ docker pull apache/doris:build-env-ldb-toolchain-latest`

    Check if the download is completed

    ```
    $ docker images
    REPOSITORY               TAG                              IMAGE ID            CREATED             SIZE
    apache/doris   build-env-ldb-toolchain-latest   49f68cecbc1a        4 days ago          3.76GB
    ```

> Note 1: For different versions of Doris, you need to download the corresponding image version. For Apache Doris 0.15 and above, the corresponding Docker image will have the same version number as Doris. For example, you can use  `apache/doris:build-env-for-0.15.0` to compile Apache Doris 0.15.0.
>
> Note 2: `apache/doris:build-env-ldb-toolchain-latest` is used to compile the latest trunk code. It will keep up with the update of the trunk code. You may view the update time in `docker/README.md`.

| Image Version | commit id | Release Version |
|---|---|---|
| apache/incubator-doris:build-env | before [ff0dd0d](https://github.com/apache/doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) | 0.8.x, 0.9.x |
| apache/incubator-doris:build-env-1.1 | [ff0dd0d](https://github.com/apache/doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) or later | 0.10.x or later |
| apache/incubator-doris:build-env-1.2 | [4ef5a8c](https://github.com/apache/doris/commit/4ef5a8c8560351d7fff7ff8fd51c4c7a75e006a8) or later | 0.12.x - 0.14.0 |
| apache/incubator-doris:build-env-1.3.1 | [ad67dd3](https://github.com/apache/doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) or later | 0.14.x |
| apache/doris:build-env-for-0.15.0 | [a81f4da](https://github.com/apache/doris/commit/a81f4da4e461a54782a96433b746d07be89e6b54) or later | 0.15.0          |
| apache/incubator-doris:build-env-latest | before [0efef1b](https://github.com/apache/doris/commit/0efef1b332300887ee0473f9df9bdd9d7297d824) | |
| apache/doris:build-env-for-1.0.0| | 1.0.0 |
| apache/doris:build-env-for-1.1.0| | 1.1.0 |
| apache/doris:build-env-for-1.2| | 1.1.x, 1.2.x |
| apache/doris:build-env-for-1.2-no-avx2| | 1.1.x, 1.2.x |
| apache/doris:build-env-for-2.0| | 2.0.x |
| apache/doris:build-env-for-2.0-no-avx2| | 2.0.x |
| apache/doris:build-env-ldb-toolchain-latest | | master |
| apache/doris:build-env-ldb-toolchain-no-avx2-latest | | mater |

**Note**:

> 1. Third-party libraries in images with "no-avx2" in their names can run on CPUs that do not support avx2 instructions. Doris can be compiled with the USE_AVX2=0 option.

> 2. Dev docker image [ChangeLog](https://github.com/apache/doris/blob/master/thirdparty/CHANGELOG.md)

> 3. For Doris 0.14.0, use `apache/incubator-doris:build-env-1.2` to compile; for Doris 0.14.x, use `apache/incubator-doris:build-env-1.3.1` to compile.

> 4. The docker images of build-env-1.3.1 and above include both OpenJDK 8 and OpenJDK 11, please confirm the default JDK version with `java -version`. You can also switch versions as follows. (It is recommended to use JDK8.)
>
>    Switch to JDK 8:
>
>    ```
>    alternatives --set java java-1.8.0-openjdk.x86_64
>    alternatives --set javac java-1.8.0-openjdk.x86_64
>    export JAVA_HOME=/usr/lib/jvm/java-1.8.0
>    ```
>
>    Switch to JDK 11:
>
>    ```
>    alternatives --set java java-11-openjdk.x86_64
>    alternatives --set javac java-11-openjdk.x86_64
>    export JAVA_HOME=/usr/lib/jvm/java-11
>    ```
>

2. Run the image

    `$ docker run -it apache/doris:build-env-ldb-toolchain-latest`

    It is recommended to run the image by mounting the local Doris source directory, so that the compiled binary file will be stored in the host machine and will not disappear because of the exit of the image.

    Meanwhile, it is recommended to mount the maven `.m2` directory in the image to the host directory to prevent repeated downloading of maven's dependent libraries each time the compilation is started.
    
    In addition, when running image compilation, it is necessary to download some files, and the image can be started in host mode. The host mode does not require the addition of - p for port mapping, as it shares network IP and ports with the host.
    
    The parameters of the docker run section are explained as follows:
    | parameter | annotation |
    |---|---|
    | -v | Mount a storage volume to a container and mount it to a directory on the container |
    | --name | Specify a container name, which can be used for container management in the future |
    | --network | Container network settings: 1 Bridge ( using the bridge specified by the Docker daemon ), 2 Host ( the network where the container uses the host ), 3 Container: NAME_ Or_ ID ( using network resources such as IP and PORT from other containers ), 4. none ( containers using their own network are similar to - net=bridge, but not configured ) |
    
    The following example refers to mounting the container's/root/doris DORIS-x.x.x-release to the host/your/local/doris DORIS-x.x.x-release directory, naming mydocker, and starting the image in host mode:

    ```
    $ docker run -it --network=host --name mydocker -v /your/local/.m2:/root/.m2 -v /your/local/doris-DORIS-x.x.x-release/:/root/doris-DORIS-x.x.x-release/ apache/doris:build-env-ldb-toolchain-latest
    ```

3. Download source code

    After starting the image, you should be in the container. You can download the Doris source code using the following command (If you have mounted the local Doris source directory, you don't need do this):

    ```
    $ git clone https://github.com/apache/doris.git
    ```

4. Compile Doris

   Firstly, run the following command to check whether the compilation machine supports the avx2 instruction set.

     ```
    $ cat /proc/cpuinfo | grep avx2
     ```

   If it is not supported, use the following command to compile.

    ```
    $ USE_AVX2=0 sh build.sh
    ```

   If supported, use the following command to compile.

    ```
    $ sh build.sh
    ```

   To build debug version for BE, add BUILD_TYPE=Debug.

    ```
    $ BUILD_TYPE=Debug sh build.sh
    ```

   After compilation, the output files will be in the `output/` directory.

    > **Note:**
     >
     > If you are using `build-env-for-0.15.0` or the subsequent versions for the first time, use the following command when compiling:
     >
     > `sh build.sh --clean --be --fe`
     >
     > This is we have upgraded the thrift (0.9 -> 0.13) for  `build-env-for-0.15.0` and the subsequent versions. That means you need to use the --clean command to force the use of the new version of thrift to generate code files, otherwise it will result in code incompatibility.

    After compilation, the output file will be in the `output/` directory.

### Self-Compile Development Environment Image

You can create a Doris development environment image yourself. Check [docker/README.md](https://github.com/apache/doris/blob/master/docker/README.md) for details.


## Direct Compilation (Ubuntu)

You can compile Doris directly in your own Linux environment.

1. System Dependencies
    * System dependencies before commit [ad67dd3](https://github.com/apache/doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) are as follows:

       `GCC 7.3+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.11+ Bison 3.0+`

       If you are using Ubuntu 16.04 or newer, you can use the following command to install the dependencies:

       ```
       sudo apt-get install build-essential openjdk-8-jdk maven byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python autopoint pkg-config
       apt-add-repository 'deb https://apt.kitware.com/ubuntu/ focal main'
       apt-get update && apt-get install cmake
       ```

       If you are using CentOS, you can use the following command to install the dependencies:

       `sudo yum groupinstall 'Development Tools' && sudo yum install maven cmake byacc flex automake libtool bison binutils-devel zip unzip ncurses-devel curl git wget python2 glibc-static libstdc++-static java-1.8.0-openjdk`

    * System dependencies after commit [ad67dd3](https://github.com/apache/doris/commit/ad67dd34a04c1ca960cff38e5b335b30fc7d559f) are as follows:

       `GCC 10+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.19.2+ Bison 3.0+`

       If you are using Ubuntu 16.04 or newer, you can use the following command to install the dependencies:

       ```
       sudo apt install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python
       sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
       sudo apt update
       sudo apt install gcc-11 g++-11
       ln -s /usr/bin/g++-11 /usr/bin/g++
       ln -s /usr/bin/gcc-11 /usr/bin/gcc
       sudo apt-get install autoconf automake libtool autopoint

2. Compile Doris

   This is the same as compiling with the Docker development image. Before compiling, you need to check whether the avx2 instruction is supported.

   ```
   $ cat /proc/cpuinfo | grep avx2
   ```

   If it is supported, use the following command to compile:

    ```
    $ sh build.sh
    ```

   If not supported, use the following command to compile:

    ```
    $ USE_AVX2=0 sh build.sh
    ```

   To build debug version for BE, add BUILD_TYPE=Debug.

    ```
    $ BUILD_TYPE=Debug sh build.sh
    ```

   After compilation, the output files will be in the `output/` directory.

## FAQ

1. `Could not transfer artifact net.sourceforge.czt.dev:cup-maven-plugin:pom:1.6-cdh from/to xxx`

    If you encounter the above error, please refer to [PR #4769](https://github.com/apache/doris/pull/4769/files) and modify the cloudera-related repo configuration in `fe/pom.xml`.

2. Invalid download links of third-party dependencies

     The download links of the third-party libraries that Doris relies on are all in the `thirdparty/vars.sh` file. Over time, some download links may fail. If you encounter this situation. It can be solved in the following two ways:

     1. Manually modify the `thirdparty/vars.sh` file

         Manually modify the problematic download links and the corresponding MD5 value.

     2. Use a third-party download warehouse:

         ```
         export REPOSITORY_URL=https://doris-thirdparty-repo.bj.bcebos.com/thirdparty
         sh thirdparty/build-thirdparty.sh
         ```

         REPOSITORY_URL contains all third-party library source code packages and their historical versions.

3. `fatal error: Killed signal terminated program ...`

   If you encounter this error, the possible reason is not enough memory allocated to the image. (The default memory allocation for Docker is 2 GB, and the peak memory usage during the compilation might exceed that.)

   You can fix this by increasing the memory allocation for the image, 4 GB ~ 8 GB, for example.

4. When using Clang to compile Doris, PCH files will be used by default to speed up the compilation process. The default configuration of ccache may cause PCH files to be unable to be cached, or the cache to be unable to be hit, resulting in PCH being repeatedly compiled, slowing down the compilation speed. The following configuration is required:  

   To use Clang to compile, but do not want to use PCH files to speed up the compilation process, you need to add the parameter `ENABLE_PCH=OFF`
   ```shell
   DORIS_TOOLCHAIN=clang ENABLE_PCH=OFF sh build.sh
   ```

## Special Statement

Starting from version 0.13, the dependency on the two third-party libraries [1] and [2] will be removed in the default compiled output. These two third-party libraries are under [GNU General Public License V3](https://www.gnu.org/licenses/gpl-3.0.en.html). This license is incompatible with [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0), so it will not appear in the Apache release by default.

Remove library [1] will result in the inability to access MySQL external tables. The feature of accessing MySQL external tables will be implemented through `UnixODBC` in future release version.

Remove library [2] will cause some data written in earlier versions (before version 0.8) to be unable to read. Because the data in the earlier version was compressed using the LZO algorithm, in later versions, it has been changed to the LZ4 compression algorithm. We will provide tools to detect and convert this part of the data in the future.

If required, you can continue to use these two libraries by adding the following option when compiling:

```
WITH_MYSQL=1 WITH_LZO=1 sh build.sh
```

Note that if you use these two third-party libraries, that means you choose not to use Doris under the Apache License 2.0, and you might need to pay attention to the GPL-related agreements.

* [1] mysql-5.7.18
* [2] lzo-2.10
