---
{
    "title": "Compiling with LDB Toolchain",
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

# Compiling with LDB-Toolchain

This topic is about how to compile Doris using the LDB toolchain. This method is an alternative to the Docker method so developers and users without a Docker environment can compile Doris from source.
The LDB toolchain version currently recommended by Doris is 0.17, which contains clang-16 and gcc-11.

> You can still compile the latest code using the Docker development image: `apache/doris:build-env-ldb-toolchain-latest`

> Special thanks to [Amos Bird](https://github.com/amosbird) for the contribution.

## Prepare the Environment

This works for most Linux distributions (CentOS, Ubuntu, etc.).

1. Download `ldb_toolchain_gen.sh`

    The latest `ldb_toolchain_gen.sh` can be downloaded from [here](https://github.com/amosbird/ldb_toolchain_gen/releases). This script is used to generate the ldb toolchain.
    
    > For more information, you can visit [https://github.com/amosbird/ldb_toolchain_gen](https://github.com/amosbird/ldb_toolchain_gen)

2. Execute the following command to generate the ldb toolchain.

    ```
    sh ldb_toolchain_gen.sh /path/to/ldb_toolchain/
    ```
    
     `/path/to/ldb_toolchain/` is the directory where the toolchain is installed.
    
    After execution, the following directory structure will be created under `/path/to/ldb_toolchain/`.
    
    ```
    ├── bin
    ├── include
    ├── lib
    ├── share
    ├── test
    └── usr
    ```
    
3. Download and install other compilation packages

    1. [Java8](https://doris-thirdparty-1308700295.cos.ap-beijing.myqcloud.com/tools/jdk-8u391-linux-x64.tar.gz)
    2. [Apache Maven 3.6.3](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/apache-maven-3.6.3-bin.tar.gz)
    3. [Node v12.13.0](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v12.13.0-linux-x64.tar.gz)

    Different Linux distributions might contain different packages, so you may need to install additional packages. The following instructions describe how to set up a minimal CentOS 6 box to compile Doris. It should work similarly for other Linux distros.

    ```
    # install required system packages
    sudo yum install -y byacc patch automake libtool make which file ncurses-devel gettext-devel unzip bzip2 zip util-linux wget git python2
    
    # install autoconf-2.69
    wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz && \
        tar zxf autoconf-2.69.tar.gz && \
        cd autoconf-2.69 && \
        ./configure && \
        make && \
        make install
    
    # install bison-3.0.4
    wget http://ftp.gnu.org/gnu/bison/bison-3.0.4.tar.gz && \
        tar xzf bison-3.0.4.tar.gz && \
        cd bison-3.0.4 && \
        ./configure && \
        make && \
        make install
    ```

4. Download Doris source code

    ```
    git clone https://github.com/apache/doris.git
    ```
    
    After downloading, create the `custom_env.sh`, file under the Doris source directory, and set the PATH environment variable:
    
    ```
    export JAVA_HOME=/path/to/java/
    export PATH=$JAVA_HOME/bin:$PATH
    export PATH=/path/to/maven/bin:$PATH
    export PATH=/path/to/node/bin:$PATH
    export PATH=/path/to/ldb_toolchain/bin:$PATH
    ```

## Compile Doris

Enter the Doris source code directory and execute:

```
$ cat /proc/cpuinfo | grep avx2
```

Check whether the compilation machine supports the avx2 instruction set.

If it is not supported, use the following command to compile:

```
$ USE_AVX2=0 sh build.sh
```

If supported, execute `sh build.sh` directly.

To build debug version for BE, add BUILD_TYPE=Debug.
```
$ BUILD_TYPE=Debug sh build.sh
```

This script will compile the third-party libraries first and then the Doris components (FE, BE) later. The compiled output will be in the `output/` directory.

## Precompile the Third-Party Binaries

The `build.sh` script will first compile the third-party dependencies. You can also directly download the precompiled three-party binaries:

`https://github.com/apache/doris-thirdparty/releases`

Here we provide precompiled third-party binaries for Linux X86(with AVX2) and MacOS(X86 Chip). If it is consistent with your compiling and running environment, you can download and use it directly.

After downloading, you will get an `installed/` directory after decompression, copy this directory to the `thirdparty/` directory, and then run `build.sh`.

