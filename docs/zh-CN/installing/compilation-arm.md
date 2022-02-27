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

## ~~使用 GCC 10 编译（已废弃）~~

该方法仅适用于 [commit 68bab73](https://github.com/apache/incubator-doris/commit/68bab73c359e40bf485a663e9a6e6ee76d81d382) 之前的 Doris 源码。

### 编译工具安装（无网络）

示例中，所有工具安装在在 `/home/doris/tools/installed/` 目录下。

所需安装包请先在有网络情况下获取。

#### 1. 安装gcc10

下载 gcc-10.1.0
    
```
wget https://mirrors.tuna.tsinghua.edu.cn/gnu/gcc/gcc-10.1.0/gcc-10.1.0.tar.gz
```
    
解压后，在 `contrib/download_prerequisites` 查看依赖并下载：

```
http://gcc.gnu.org/pub/gcc/infrastructure/gmp-6.1.0.tar.bz2
http://gcc.gnu.org/pub/gcc/infrastructure/mpfr-3.1.4.tar.bz2
http://gcc.gnu.org/pub/gcc/infrastructure/mpc-1.0.3.tar.gz
http://gcc.gnu.org/pub/gcc/infrastructure/isl-0.18.tar.bz2
```
   
解压这四个依赖，然后移动到 gcc-10.1.0 源码目录下，并重命名为 gmp、isl、mpc、mpfr。

下载并安装 automake-1.15（因为gcc10编译过程中会查找automake 1.15 版本）
    
```
https://ftp.gnu.org/gnu/automake/automake-1.15.tar.gz
tar xzf automake-1.15.tar.gz
./configure --prefix=/home/doris/tools/installed
make && make install
export PATH=/home/doris/tools/installed/bin:$PATH
```

编译GCC10:

```
cd gcc-10.1.0
./configure --prefix=/home/doris/tools/installed
make -j && make install
```

编译时间较长。

#### 2. 安装其他编译组件

1. jdk-8u291-linux-aarch64.tar.gz

    `https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html`
    
    无需编译，开箱即用。
        
2. cmake-3.19.8-Linux-aarch64.tar.gz

    `https://cmake.org/download/`
    
    无需编译，开箱即用
    
3. apache-maven-3.8.1-bin.tar.gz

    `https://maven.apache.org/download.cgi`
    
    无需编译，开箱即用
    
4. nodejs 16.3.0

    `https://nodejs.org/dist/v16.3.0/node-v16.3.0-linux-arm64.tar.xz`
    
    无需编译，开箱即用
    
5. libtool-2.4.6.tar.gz

    编译第三方组件用，虽然系统可能自带了libtool，但是libtool需要和automake在一起，这样不容易出问题。

    ```
    https://ftp.gnu.org/gnu/libtool/libtool-2.4.6.tar.gz
    cd  libtool-2.4.6/
    ./configure --prefix=/home/doris/tools/installed
    make -j && make install
    ```
    
6. binutils-2.36.tar.xz（获取bdf.h）

    ```
    https://ftp.gnu.org/gnu/binutils/binutils-2.36.tar.bz2
    ./configure --prefix=/home/doris/tools/installed
    make -j && make install
    ```
    
7. libiberty（编译BE用）

    这个库的源码就在 gcc-10.1.0 的源码包下
    ```
    cd gcc-10.1.0/libiberty/
    ./configure --prefix=/home/doris/tools/installed
    make
    ```
    
    编译后会产生 libiberty.a，后续移动到 Doris 的thirdparty 的 lib64 目录中即可。
    
#### 3. 编译第三方库

假设Doris源码在 `/home/doris/doris-src/` 下。

1. 手动下载所有第三方库并放在 thirdparty/src 目录下。
2. 在Doris源码目录下新增 `custom_env.sh` 并添加如下内容

    ```
    export DORIS_THIRDPARTY=/home/doris/doris-src/thirdparty/
    export JAVA_HOME=/home/doris/tools/jdk1.8.0_291/
    export DORIS_GCC_HOME=/home/doris/tools/installed/
    export PATCH_COMPILER_RT=true
    ```
    
    注意替换对应的目录
    
3. 修改 build-thirdparty.sh 中的部分内容

    1.  关闭 `build_mysql` 和 `build_libhdfs3`

        mysql 不再需要。而 libhdfs3 暂不支持 arm 架构，所以在arm中运行Doris，暂不支持通过 libhdfs3 直接访问 hdfs，需要通过broker。
        
    2. 在 `build_curl` 中增加 configure 参数：`--without-libpsl`。如果不添加，则在最终编译Doris BE的链接阶段，可能报错：`undefined reference to ‘psl_is_cookie_domain_acceptable'`
    
4. 执行 build-thirdparty.sh。这里仅列举可能出现的错误

    * ` error: narrowing conversion of '-1' from 'int' to 'char' [-Wnarrowing]`

        编译brpc 0.9.7 时会出现错误，解决方案，在 brpc 的 CMakeLists.txt 的 `CMAKE_CXX_FLAGS` 中添加 `-Wno-narrowing`。brpc master 代码中已经修复这个问题：
        
        `https://github.com/apache/incubator-brpc/issues/1091`
        
    * `libz.a(deflate.o): relocation R_AARCH64_ADR_PREL_PG_HI21 against symbol `z_errmsg' which may bind externally can not be used when making a shared object; recompile with -fPIC`

        编译brpc 0.9.7 时会出现错误，还有 libcrypto 也会报类似错误。原因未知，似乎在 aarch64 下，brpc 需要链接动态的 zlib 和 crypto 库。但是我们在编译这两个第三方库时，都只编译的了 .a 静态文件。解决方案：重新编译zlib和 openssl 生成.so 动态库：
        
        打开 `build-thirdparty.sh`，找到 `build_zlib` 函数，将：
        
        ```
        ./configure --prefix=$TP_INSTALL_DIR --static
        就改为
        ./configure --prefix=$TP_INSTALL_DIR
        ```
        
        找到 `build_openssl`，将以下部分注释掉：
        
        ```
        #if [ -f $TP_INSTALL_DIR/lib64/libcrypto.so ]; then
        #    rm -rf $TP_INSTALL_DIR/lib64/libcrypto.so*
        #fi
        #if [ -f $TP_INSTALL_DIR/lib64/libssl.so ]; then
        #    rm -rf $TP_INSTALL_DIR/lib64/libssl.so*
        #fi
        ```
        
        然后来到 `build-thirdparty.sh`，注释掉其他 `build_xxx`，仅打开 `build_zlib` 和 `build_openssl`，以及 `build_brpc` 和之后的 `build_xxx`。然后重新执行 `build-thirdparty.sh`。

    * 编译到某个阶段卡住不动。

        不确定原因。解决方案：重跑 `build-thirdparty.sh`。`build-thirdparty.sh` 是可以重复执行的。

#### 4. 编译Doris源码

执行 `sh build.sh` 即可。

#### 5. 常见错误

1. 编译 Doris 时出现 `undefined reference to psl_free`

    libcurl 会调用 libpsl 的函数，但 libpsl 未连接，原因未知。解决方法（二选一）：

    1. 在 `thirdparty/build-thirdparty.sh` 中的 `build_curl` 方法中添加 `--without-libpsl` 后重新编译 libcurl，然后再重新编译 Doris。
    2. `be/CMakeLists.txt` 中 603 行左右，`-pthread` 后添加 `-lpsl`，然后重新编译 Doris。
