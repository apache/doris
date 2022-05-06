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

## ~~ Compile with GCC 10 (deprecated) ~~

This method only works with Doris source code before [commit 68bab73](https://github.com/apache/incubator-doris/commit/68bab73c359e40bf485a663e9a6e6ee76d81d382).

### Compilation tool installation (no network)

In the example, all tools are installed in the `/home/doris/tools/installed/` directory.

Please obtain the required installation package first under network conditions.

#### 1. Install gcc10

Download gcc-10.1.0

```
wget https://mirrors.tuna.tsinghua.edu.cn/gnu/gcc/gcc-10.1.0/gcc-10.1.0.tar.gz
```

After unzipping, check the dependencies in `contrib/download_prerequisites` and download:

```
http://gcc.gnu.org/pub/gcc/infrastructure/gmp-6.1.0.tar.bz2
http://gcc.gnu.org/pub/gcc/infrastructure/mpfr-3.1.4.tar.bz2
http://gcc.gnu.org/pub/gcc/infrastructure/mpc-1.0.3.tar.gz
http://gcc.gnu.org/pub/gcc/infrastructure/isl-0.18.tar.bz2
```

Unzip these four dependencies, then move to the gcc-10.1.0 source directory and rename them to gmp, isl, mpc, mpfr.

Download and install automake-1.15 (because gcc10 will find automake 1.15 version during compilation)

```
https://ftp.gnu.org/gnu/automake/automake-1.15.tar.gz
tar xzf automake-1.15.tar.gz
./configure --prefix=/home/doris/tools/installed
make && make install
export PATH=/home/doris/tools/installed/bin:$PATH
```

Compile GCC10:

```
cd gcc-10.1.0
./configure --prefix=/home/doris/tools/installed
make -j && make install
```

Compile time is longer.

#### 2. Install other compilation components

1. jdk-8u291-linux-aarch64.tar.gz

    `https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html`

    No need to compile, just use it out of the box.

2. cmake-3.19.8-Linux-aarch64.tar.gz

    `https://cmake.org/download/`

    No need to compile, just use it out of the box

3. apache-maven-3.8.1-bin.tar.gz

    `https://maven.apache.org/download.cgi`

    No need to compile, just use it out of the box

4. nodejs 16.3.0

    `https://nodejs.org/dist/v16.3.0/node-v16.3.0-linux-arm64.tar.xz`

    No need to compile, just use it out of the box

5. libtool-2.4.6.tar.gz

    For compiling third-party components, although the system may come with libtool, libtool needs to be together with automake, so it is not easy to cause problems.

    ```
    https://ftp.gnu.org/gnu/libtool/libtool-2.4.6.tar.gz
    cd libtool-2.4.6/
    ./configure --prefix=/home/doris/tools/installed
    make -j && make install
    ```

6. binutils-2.36.tar.xz (obtain bdf.h)

    ```
    https://ftp.gnu.org/gnu/binutils/binutils-2.36.tar.bz2
    ./configure --prefix=/home/doris/tools/installed
    make -j && make install
    ```

7. Libiberty (for compiling BE)

    The source code of this library is under the source code package of gcc-10.1.0
    ```
    cd gcc-10.1.0/libiberty/
    ./configure --prefix=/home/doris/tools/installed
    make
    ```

    After compilation, libiberty.a will be generated, which can be moved to the lib64 directory of Doris' thirdparty.

#### 3. Compile third-party libraries

Suppose Doris source code is under `/home/doris/doris-src/`.

1. Manually download all third-party libraries and place them in the thirdparty/src directory.
2. Add `custom_env.sh` in the Doris source directory and add the following content

    ```
    export DORIS_THIRDPARTY=/home/doris/doris-src/thirdparty/
    export JAVA_HOME=/home/doris/tools/jdk1.8.0_291/
    export DORIS_GCC_HOME=/home/doris/tools/installed/
    export PATCH_COMPILER_RT=true
    ```

    Pay attention to replace the corresponding directory

3. Modify part of the content in build-thirdparty.sh

    1. Close `build_mysql` and `build_libhdfs3`

        mysql is no longer needed. However, libhdfs3 does not support arm architecture for the time being, so running Doris in arm does not support direct access to hdfs through libhdfs3, and requires a broker.

    2. Add the configure parameter in `build_curl`: `--without-libpsl`. If it is not added, an error may be reported during the linking phase of the final compilation of Doris BE: `undefined reference to ‘psl_is_cookie_domain_acceptable'`

4. Execute build-thirdparty.sh. Here are only possible errors

    * `error: narrowing conversion of'-1' from'int' to'char' [-Wnarrowing]`

        There will be an error when compiling brpc 0.9.7. The solution is to add `-Wno-narrowing` in `CMAKE_CXX_FLAGS` of CMakeLists.txt of brpc. This problem has been fixed in the brpc master code:

        `https://github.com/apache/incubator-brpc/issues/1091`

    * `libz.a(deflate.o): relocation R_AARCH64_ADR_PREL_PG_HI21 against symbol `z_errmsg' which may bind externally can not be used when making a shared object; recompile with -fPIC`

        There will be errors when compiling brpc 0.9.7, and libcrypto will also report similar errors. The reason is unknown. It seems that under aarch64, brpc needs to link the dynamic zlib and crypto libraries. But when we compile these two third-party libraries, we only compiled .a static files. Solution: Recompile zlib and openssl to generate .so dynamic library:

        Open `build-thirdparty.sh`, find the `build_zlib` function, and change:

        ```
        ./configure --prefix=$TP_INSTALL_DIR --static
        Just change to
        ./configure --prefix=$TP_INSTALL_DIR
        ```

        Find `build_openssl` and comment out the following parts:

        ```
        #if [-f $TP_INSTALL_DIR/lib64/libcrypto.so ]; then
        # rm -rf $TP_INSTALL_DIR/lib64/libcrypto.so*
        #fi
        #if [-f $TP_INSTALL_DIR/lib64/libssl.so ]; then
        # rm -rf $TP_INSTALL_DIR/lib64/libssl.so*
        #fi
        ```

         Then go to `build-thirdparty.sh`, comment out other `build_xxx`, open only `build_zlib` and `build_openssl`, and `build_brpc` and later `build_xxx`. Then re-execute `build-thirdparty.sh`.

     * The compilation is stuck at a certain stage.

         Not sure why. Solution: Rerun `build-thirdparty.sh`. `build-thirdparty.sh` can be executed repeatedly.

#### 4. Compile Doris source code

Execute `sh build.sh`.

#### 5. FAQ

1. `undefined reference to psl_free` appears when compiling Doris

     libcurl will call libpsl functions, but libpsl is not linked for an unknown reason. Solutions (choose one of the two):

     1. Add `--without-libpsl` to the `build_curl` method in `thirdparty/build-thirdparty.sh`, recompile libcurl, and then recompile Doris.
     2. About line 603 in `be/CMakeLists.txt`, add `-lpsl` after `-pthread`, and then recompile Doris.
