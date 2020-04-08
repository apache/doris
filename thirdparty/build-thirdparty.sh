#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#################################################################################
# This script will
# 1. Check prerequisite libraries. Including:
#    cmake byacc flex automake libtool binutils-dev libiberty-dev bison
# 2. Compile and install all thirdparties which are downloaded
#    using *download-thirdparty.sh*.
#
# This script will run *download-thirdparty.sh* once again
# to check if all thirdparties have been downloaded, unpacked and patched.
#################################################################################
set -e

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

export DORIS_HOME=$curdir/..
export TP_DIR=$curdir

# include custom environment variables
if [[ -f ${DORIS_HOME}/env.sh ]]; then
    . ${DORIS_HOME}/env.sh
fi

if [[ ! -f ${TP_DIR}/download-thirdparty.sh ]]; then
    echo "Download thirdparty script is missing".
    exit 1
fi

if [ ! -f ${TP_DIR}/vars.sh ]; then
    echo "vars.sh is missing".
    exit 1
fi
. ${TP_DIR}/vars.sh

cd $TP_DIR

# Download thirdparties.
${TP_DIR}/download-thirdparty.sh

export LD_LIBRARY_PATH=$TP_DIR/installed/lib:$LD_LIBRARY_PATH

# set COMPILER
if [[ ! -z ${DORIS_GCC_HOME} ]]; then
    export CC=${DORIS_GCC_HOME}/bin/gcc
    export CPP=${DORIS_GCC_HOME}/bin/cpp
    export CXX=${DORIS_GCC_HOME}/bin/g++
else
    echo "DORIS_GCC_HOME environment variable is not set"
    exit 1
fi

# prepare installed prefix
mkdir -p ${TP_DIR}/installed

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! $CMD; then
        echo $NAME is missing
        exit 1
    else
        echo $NAME is found
    fi
}

# sudo apt-get install cmake
# sudo yum install cmake
check_prerequest "${CMAKE_CMD} --version" "cmake"

# sudo apt-get install byacc
# sudo yum install byacc
check_prerequest "byacc -V" "byacc"

# sudo apt-get install flex
# sudo yum install flex
check_prerequest "flex -V" "flex"

# sudo apt-get install automake
# sudo yum install automake
check_prerequest "automake --version" "automake"

# sudo apt-get install libtool
# sudo yum install libtool
check_prerequest "libtoolize --version" "libtool"

# sudo apt-get install binutils-dev
# sudo yum install binutils-devel
#check_prerequest "locate libbfd.a" "binutils-dev"

# sudo apt-get install libiberty-dev
# no need in centos 7.1
#check_prerequest "locate libiberty.a" "libiberty-dev"

# sudo apt-get install bison
# sudo yum install bison
#check_prerequest "bison --version" "bison"

#########################
# build all thirdparties
#########################

check_if_source_exist() {
    if [ -z $1 ]; then
        echo "dir should specified to check if exist."
        exit 1
    fi

    if [ ! -d $TP_SOURCE_DIR/$1 ];then
        echo "$TP_SOURCE_DIR/$1 does not exist."
        exit 1
    fi
    echo "===== begin build $1"
}

check_if_archieve_exist() {
    if [ -z $1 ]; then
        echo "archieve should specified to check if exist."
        exit 1
    fi

    if [ ! -f $TP_SOURCE_DIR/$1 ];then
        echo "$TP_SOURCE_DIR/$1 does not exist."
        exit 1
    fi
}

# libevent
build_libevent() {
    check_if_source_exist $LIBEVENT_SOURCE
    cd $TP_SOURCE_DIR/$LIBEVENT_SOURCE
    if [ ! -f configure ]; then
        ./autogen.sh
    fi

    CFLAGS="-std=c99 -fPIC -D_BSD_SOURCE -fno-omit-frame-pointer -g -ggdb -O2 -I${TP_INCLUDE_DIR}" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    ./configure --prefix=$TP_INSTALL_DIR --enable-shared=no --disable-samples --disable-libevent-regress
    make -j$PARALLEL && make install
}

build_openssl() {
    MACHINE_TYPE=$(uname -m)
    OPENSSL_PLATFORM="linux-x86_64"
    if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
        OPENSSL_PLATFORM="linux-aarch64"
    fi

    check_if_source_exist $OPENSSL_SOURCE
    cd $TP_SOURCE_DIR/$OPENSSL_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR} -fPIC" \
    CXXFLAGS="-I${TP_INCLUDE_DIR} -fPIC" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    LIBDIR="lib" \
    ./Configure --prefix=$TP_INSTALL_DIR -zlib -shared ${OPENSSL_PLATFORM}
    make -j$PARALLEL && make install
    if [ -f $TP_INSTALL_DIR/lib64/libcrypto.a ]; then
        mkdir -p $TP_INSTALL_DIR/lib && \
        cp $TP_INSTALL_DIR/lib64/libcrypto.a $TP_INSTALL_DIR/lib/libcrypto.a && \
        cp $TP_INSTALL_DIR/lib64/libssl.a $TP_INSTALL_DIR/lib/libssl.a
    fi
    # NOTE(zc): remove this dynamic library files to make libcurl static link.
    # If I don't remove this files, I don't known how to make libcurl link static library
    if [ -f $TP_INSTALL_DIR/lib64/libcrypto.so ]; then
        rm -rf $TP_INSTALL_DIR/lib64/libcrypto.so*
    fi
    if [ -f $TP_INSTALL_DIR/lib64/libssl.so ]; then
        rm -rf $TP_INSTALL_DIR/lib64/libssl.so*
    fi
    if [ -f $TP_INSTALL_DIR/lib/libcrypto.so ]; then
        rm -rf $TP_INSTALL_DIR/lib/libcrypto.so*
    fi
    if [ -f $TP_INSTALL_DIR/lib/libssl.so ]; then
        rm -rf $TP_INSTALL_DIR/lib/libssl.so*
    fi
}

# thrift
build_thrift() {
    check_if_source_exist $THRIFT_SOURCE
    cd $TP_SOURCE_DIR/$THRIFT_SOURCE

    if [ ! -f configure ]; then
        ./bootstrap.sh
    fi

    echo ${TP_LIB_DIR}
    ./configure CPPFLAGS="-I${TP_INCLUDE_DIR}" LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" LIBS="-lcrypto -ldl -lssl" CFLAGS="-fPIC" \
    --prefix=$TP_INSTALL_DIR --docdir=$TP_INSTALL_DIR/doc --enable-static --disable-shared --disable-tests \
    --disable-tutorial --without-qt4 --without-qt5 --without-csharp --without-erlang --without-nodejs \
    --without-lua --without-perl --without-php --without-php_extension --without-dart --without-ruby \
    --without-haskell --without-go --without-haxe --without-d --without-python -without-java --with-cpp \
    --with-libevent=$TP_INSTALL_DIR --with-boost=$TP_INSTALL_DIR --with-openssl=$TP_INSTALL_DIR

    if [ -f compiler/cpp/thrifty.hh ];then
        mv compiler/cpp/thrifty.hh compiler/cpp/thrifty.h
    fi

    make -j$PARALLEL && make install
}

# llvm
build_llvm() {
    MACHINE_TYPE=$(uname -m)
    LLVM_TARGET="X86"
    if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
        LLVM_TARGET="AArch64"
    fi

    check_if_source_exist $LLVM_SOURCE
    check_if_source_exist $CLANG_SOURCE
    check_if_source_exist $COMPILER_RT_SOURCE

    if [ ! -d $TP_SOURCE_DIR/$LLVM_SOURCE/tools/clang ]; then
        cp -rf $TP_SOURCE_DIR/$CLANG_SOURCE $TP_SOURCE_DIR/$LLVM_SOURCE/tools/clang
    fi

    if [ ! -d $TP_SOURCE_DIR/$LLVM_SOURCE/projects/compiler-rt ]; then
        cp -rf $TP_SOURCE_DIR/$COMPILER_RT_SOURCE $TP_SOURCE_DIR/$LLVM_SOURCE/projects/compiler-rt
    fi

    cd $TP_SOURCE_DIR
    mkdir llvm-build -p && cd llvm-build
    rm -rf CMakeCache.txt CMakeFiles/
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    $CMAKE_CMD -DLLVM_REQUIRES_RTTI:Bool=True -DLLVM_TARGETS_TO_BUILD=${LLVM_TARGET} -DLLVM_ENABLE_TERMINFO=OFF LLVM_BUILD_LLVM_DYLIB:BOOL=OFF -DLLVM_ENABLE_PIC=true -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE="RELEASE" -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR/llvm ../$LLVM_SOURCE
    make -j$PARALLEL REQUIRES_RTTI=1 && make install
}

# protobuf
build_protobuf() {
    check_if_source_exist $PROTOBUF_SOURCE
    cd $TP_SOURCE_DIR/$PROTOBUF_SOURCE
    rm -fr gmock
    mkdir gmock && cd gmock && tar xf ${TP_SOURCE_DIR}/googletest-release-1.8.0.tar.gz \
    && mv googletest-release-1.8.0 gtest && cd $TP_SOURCE_DIR/$PROTOBUF_SOURCE && ./autogen.sh
    CXXFLAGS="-fPIC -O2 -I ${TP_INCLUDE_DIR}" \
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    ./configure --prefix=${TP_INSTALL_DIR} --disable-shared --enable-static --with-zlib=${TP_INSTALL_DIR}/include
    cd src
    sed -i 's/^AM_LDFLAGS\(.*\)$/AM_LDFLAGS\1 -all-static/' Makefile
    cd -
    make -j$PARALLEL && make install
}

# gflags
build_gflags() {
    check_if_source_exist $GFLAGS_SOURCE

    cd $TP_SOURCE_DIR/$GFLAGS_SOURCE
    mkdir build -p && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On ../
    make -j$PARALLEL && make install
}

# glog
build_glog() {
    check_if_source_exist $GLOG_SOURCE
    cd $TP_SOURCE_DIR/$GLOG_SOURCE

    # to generate config.guess and config.sub to support aarch64
    rm -rf config.*
    autoreconf -i

    CPPFLAGS="-I${TP_INCLUDE_DIR} -fpermissive -fPIC" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR --enable-frame-pointers --disable-shared --enable-static
    make -j$PARALLEL && make install
}

# gtest
build_gtest() {
    check_if_source_exist $GTEST_SOURCE

    cd $TP_SOURCE_DIR/$GTEST_SOURCE
    mkdir build -p && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On ../
    make -j$PARALLEL && make install
}

# rapidjson
build_rapidjson() {
    check_if_source_exist $RAPIDJSON_SOURCE

    rm $TP_INSTALL_DIR/rapidjson -rf
    cp $TP_SOURCE_DIR/$RAPIDJSON_SOURCE/include/rapidjson $TP_INCLUDE_DIR/ -r
}

# snappy
build_snappy() {
    check_if_source_exist $SNAPPY_SOURCE
    cd $TP_SOURCE_DIR/$SNAPPY_SOURCE

    mkdir build -p && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    CFLAGS="-O3" CXXFLAGS="-O3" $CMAKE_CMD -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INSTALL_LIBDIR=lib64 \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On \
    -DCMAKE_INSTALL_INCLUDEDIR=$TP_INCLUDE_DIR/snappy \
    -DSNAPPY_BUILD_TESTS=0 ../
    make -j$PARALLEL && make install
    if [ -f $TP_INSTALL_DIR/lib64/libsnappy.a ]; then
        mkdir -p $TP_INSTALL_DIR/lib && cp $TP_INSTALL_DIR/lib64/libsnappy.a $TP_INSTALL_DIR/lib/libsnappy.a

    fi

    #build for libarrow.a
    cp $TP_INCLUDE_DIR/snappy/snappy-c.h  $TP_INCLUDE_DIR/snappy-c.h && \
    cp $TP_INCLUDE_DIR/snappy/snappy-sinksource.h  $TP_INCLUDE_DIR/snappy-sinksource.h && \
    cp $TP_INCLUDE_DIR/snappy/snappy-stubs-public.h  $TP_INCLUDE_DIR/snappy-stubs-public.h && \
    cp $TP_INCLUDE_DIR/snappy/snappy.h  $TP_INCLUDE_DIR/snappy.h && \
    cp $TP_INSTALL_DIR/lib/libsnappy.a $TP_INSTALL_DIR/libsnappy.a
}

# gperftools
build_gperftools() {
    check_if_source_exist $GPERFTOOLS_SOURCE
    cd $TP_SOURCE_DIR/$GPERFTOOLS_SOURCE
    if [ ! -f configure ]; then
        ./autogen.sh
    fi

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    LD_LIBRARY_PATH="${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    LD_LIBRARY_PATH="${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR/gperftools --disable-shared --enable-static --disable-libunwind --with-pic --enable-frame-pointers
    make -j$PARALLEL && make install
}

# zlib
build_zlib() {
    check_if_source_exist $ZLIB_SOURCE
    cd $TP_SOURCE_DIR/$ZLIB_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR --static
    make -j$PARALLEL && make install
}

# lz4
build_lz4() {
    check_if_source_exist $LZ4_SOURCE
    cd $TP_SOURCE_DIR/$LZ4_SOURCE

    make -j$PARALLEL install PREFIX=$TP_INSTALL_DIR \
    INCLUDEDIR=$TP_INCLUDE_DIR/lz4/
}

# bzip
build_bzip() {
    check_if_source_exist $BZIP_SOURCE
    cd $TP_SOURCE_DIR/$BZIP_SOURCE

    CFLAGS="-fPIC"
    make -j$PARALLEL install PREFIX=$TP_INSTALL_DIR
}

# lzo2
build_lzo2() {
    check_if_source_exist $LZO2_SOURCE
    cd $TP_SOURCE_DIR/$LZO2_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR} -fPIC" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR --disable-shared --enable-static
    make -j$PARALLEL && make install
}

# curl
build_curl() {
    check_if_source_exist $CURL_SOURCE
    cd $TP_SOURCE_DIR/$CURL_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
    LDFLAGS="-L${TP_LIB_DIR}" LIBS="-lcrypto -lssl -lcrypto -ldl" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR --disable-shared --enable-static \
    --without-librtmp --with-ssl=${TP_INSTALL_DIR} --without-libidn2 --disable-ldap --enable-ipv6
    make -j$PARALLEL && make install
}

# re2
build_re2() {
    check_if_source_exist $RE2_SOURCE
    cd $TP_SOURCE_DIR/$RE2_SOURCE

    $CMAKE_CMD -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR
    make -j$PARALLEL install
}

# boost
build_boost() {
    check_if_source_exist $BOOST_SOURCE
    cd $TP_SOURCE_DIR/$BOOST_SOURCE

    echo "using gcc : doris : ${CXX} ; " > tools/build/src/user-config.jam
    ./bootstrap.sh --prefix=$TP_INSTALL_DIR
    ./b2 --toolset=gcc-doris link=static -d0 -j$PARALLEL --without-mpi --without-graph --without-graph_parallel --without-python cxxflags="-std=c++11 -fPIC -I$TP_INCLUDE_DIR -L$TP_LIB_DIR" install
}

# mysql
build_mysql() {
    check_if_source_exist $MYSQL_SOURCE
    check_if_source_exist $BOOST_FOR_MYSQL_SOURCE

    cd $TP_SOURCE_DIR/$MYSQL_SOURCE

    mkdir build -p && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    if [ ! -d $BOOST_FOR_MYSQL_SOURCE ]; then
        cp $TP_SOURCE_DIR/$BOOST_FOR_MYSQL_SOURCE ./ -rf
    fi

    $CMAKE_CMD ../ -DWITH_BOOST=`pwd`/$BOOST_FOR_MYSQL_SOURCE -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR/mysql/ \
    -DCMAKE_INCLUDE_PATH=$TP_INCLUDE_DIR -DCMAKE_LIBRARY_PATH=$TP_LIB_DIR -DWITHOUT_SERVER=1 \
    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O3 -g -fabi-version=2 -fno-omit-frame-pointer -fno-strict-aliasing -std=gnu++11" \
    -DDISABLE_SHARED=1 -DBUILD_SHARED_LIBS=0
    make -j$PARALLEL mysqlclient

    # copy headers manually
    rm ../../../installed/include/mysql/ -rf
    mkdir ../../../installed/include/mysql/ -p
    cp -R ./include/* ../../../installed/include/mysql/
    cp -R ../include/* ../../../installed/include/mysql/
    cp ../libbinlogevents/export/binary_log_types.h ../../../installed/include/mysql/
    echo "mysql headers are installed."

    # copy libmysqlclient.a
    cp libmysql/libmysqlclient.a ../../../installed/lib/
    echo "mysql client lib is installed."
}

#leveldb
build_leveldb() {
    check_if_source_exist $LEVELDB_SOURCE
    cd $TP_SOURCE_DIR/$LEVELDB_SOURCE
    CXXFLAGS="-fPIC" make -j$PARALLEL
    cp out-static/libleveldb.a ../../installed/lib/libleveldb.a
    cp -r include/leveldb ../../installed/include/
}

# brpc
build_brpc() {
    check_if_source_exist $BRPC_SOURCE

    cd $TP_SOURCE_DIR/$BRPC_SOURCE
    mkdir build -p && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    $CMAKE_CMD -v -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DBRPC_WITH_GLOG=ON -DWITH_GLOG=ON -DCMAKE_INCLUDE_PATH="$TP_INSTALL_DIR/include" \
    -DCMAKE_LIBRARY_PATH="$TP_INSTALL_DIR/lib;$TP_INSTALL_DIR/lib64" \
    -DPROTOBUF_PROTOC_EXECUTABLE=$TP_INSTALL_DIR/bin/protoc \
    -DProtobuf_PROTOC_EXECUTABLE=$TP_INSTALL_DIR/bin/protoc ..
    make -j$PARALLEL && make install
    if [ -f $TP_INSTALL_DIR/lib/libbrpc.a ]; then
        mkdir -p $TP_INSTALL_DIR/lib64 && cp $TP_INSTALL_DIR/lib/libbrpc.a $TP_INSTALL_DIR/lib64/libbrpc.a
    fi
}

# rocksdb
build_rocksdb() {
    check_if_source_exist $ROCKSDB_SOURCE

    cd $TP_SOURCE_DIR/$ROCKSDB_SOURCE

    CFLAGS="-I ${TP_INCLUDE_DIR} -I ${TP_INCLUDE_DIR}/snappy -I ${TP_INCLUDE_DIR}/lz4" CXXFLAGS="-fPIC -Wno-deprecated-copy -Wno-stringop-truncation -Wno-pessimizing-move" LDFLAGS="-static-libstdc++ -static-libgcc" \
        PORTABLE=1 make USE_RTTI=1 -j$PARALLEL static_lib
    cp librocksdb.a ../../installed/lib/librocksdb.a
    cp -r include/rocksdb ../../installed/include/
}

# librdkafka
build_librdkafka() {
    check_if_source_exist $LIBRDKAFKA_SOURCE

    cd $TP_SOURCE_DIR/$LIBRDKAFKA_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR --enable-static --disable-sasl
    make -j$PARALLEL && make install
}

# flatbuffers
build_flatbuffers() {
  check_if_source_exist $FLATBUFFERS_SOURCE
  cd $TP_SOURCE_DIR/$FLATBUFFERS_SOURCE
  mkdir build -p && cd build
  rm -rf CMakeCache.txt CMakeFiles/
  CXXFLAGS="-fPIC -Wno-class-memaccess" \
  LDFLAGS="-static-libstdc++ -static-libgcc" \
  ${CMAKE_CMD} ..
  make -j$PARALLEL
  cp flatc  ../../../installed/bin/flatc
  cp -r ../include/flatbuffers  ../../../installed/include/flatbuffers
  cp libflatbuffers.a ../../../installed/lib/libflatbuffers.a
}

# arrow
build_arrow() {
    check_if_source_exist $ARROW_SOURCE
    cd $TP_SOURCE_DIR/$ARROW_SOURCE/cpp && mkdir -p release && cd release
    export ARROW_BROTLI_URL=${TP_SOURCE_DIR}/${BROTLI_NAME}
    export ARROW_DOUBLE_CONVERSION_URL=${TP_SOURCE_DIR}/${DOUBLE_CONVERSION_NAME}
    export ARROW_GLOG_URL=${TP_SOURCE_DIR}/${GLOG_NAME}
    export ARROW_LZ4_URL=${TP_SOURCE_DIR}/${LZ4_NAME}
    export ARROW_FLATBUFFERS_URL=${TP_SOURCE_DIR}/${FLATBUFFERS_NAME}
    export ARROW_ZSTD_URL=${TP_SOURCE_DIR}/${ZSTD_NAME}
    export ARROW_JEMALLOC_URL=${TP_SOURCE_DIR}/${JEMALLOC_NAME}
    export LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc"

    ${CMAKE_CMD} -DARROW_PARQUET=ON -DARROW_IPC=ON -DARROW_USE_GLOG=off -DARROW_BUILD_SHARED=OFF \
    -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INSTALL_LIBDIR=lib64 \
    -DARROW_BOOST_USE_SHARED=OFF -DARROW_GFLAGS_USE_SHARED=OFF -DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$TP_INSTALL_DIR \
    -Dgflags_ROOT=$TP_INSTALL_DIR/ \
    -DSnappy_ROOT=$TP_INSTALL_DIR/ \
    -DGLOG_ROOT=$TP_INSTALL_DIR/ \
    -DLZ4_ROOT=$TP_INSTALL_DIR/ \
    -DThrift_ROOT=$TP_INSTALL_DIR/ ..

    make -j$PARALLEL && make install
    #copy dep libs
    cp -rf ./jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/libjemalloc_pic.a $TP_INSTALL_DIR/lib64/libjemalloc.a
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlienc-static.a $TP_INSTALL_DIR/lib64/libbrotlienc.a
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlidec-static.a $TP_INSTALL_DIR/lib64/libbrotlidec.a
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlicommon-static.a $TP_INSTALL_DIR/lib64/libbrotlicommon.a
    if [ -f ./zstd_ep-install/lib64/libzstd.a ]; then
        cp -rf ./zstd_ep-install/lib64/libzstd.a $TP_INSTALL_DIR/lib64/libzstd.a
    else
        cp -rf ./zstd_ep-install/lib/libzstd.a $TP_INSTALL_DIR/lib64/libzstd.a
    fi
    cp -rf ./double-conversion_ep/src/double-conversion_ep/lib/libdouble-conversion.a $TP_INSTALL_DIR/lib64/libdouble-conversion.a
}

# s2
build_s2() {
    check_if_source_exist $S2_SOURCE
    cd $TP_SOURCE_DIR/s2geometry-0.9.0
    mkdir build -p && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    CXXFLAGS="-O3" \
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    $CMAKE_CMD -v -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INCLUDE_PATH="$TP_INSTALL_DIR/include" \
    -DBUILD_SHARED_LIBS=OFF \
    -DGFLAGS_ROOT_DIR="$TP_INSTALL_DIR/include" \
    -DWITH_GFLAGS=ON \
    -DGLOG_ROOT_DIR="$TP_INSTALL_DIR/include" \
    -DWITH_GLOG=ON \
    -DCMAKE_LIBRARY_PATH="$TP_INSTALL_DIR/lib;$TP_INSTALL_DIR/lib64" ..
    make -j$PARALLEL && make install
}

# bitshuffle
build_bitshuffle() {
    check_if_source_exist $BITSHUFFLE_SOURCE
    cd $TP_SOURCE_DIR/$BITSHUFFLE_SOURCE
    PREFIX=$TP_INSTALL_DIR

    # This library has significant optimizations when built with -mavx2. However,
    # we still need to support non-AVX2-capable hardware. So, we build it twice,
    # once with the flag and once without, and use some linker tricks to
    # suffix the AVX2 symbols with '_avx2'.
    arches="default avx2"
    MACHINE_TYPE=$(uname -m)
    # Becuase aarch64 don't support avx2, disable it.
    if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
        arches="default"
    fi

    to_link=""
    for arch in $arches ; do
        arch_flag=""
        if [ "$arch" == "avx2" ]; then
            arch_flag="-mavx2"
        fi
        tmp_obj=bitshuffle_${arch}_tmp.o
        dst_obj=bitshuffle_${arch}.o
        ${CC:-gcc} $EXTRA_CFLAGS $arch_flag -std=c99 -I$PREFIX/include/lz4/ -O3 -DNDEBUG -fPIC -c \
            "src/bitshuffle_core.c" \
            "src/bitshuffle.c" \
            "src/iochain.c"
        # Merge the object files together to produce a combined .o file.
        ld -r -o $tmp_obj bitshuffle_core.o bitshuffle.o iochain.o
        # For the AVX2 symbols, suffix them.
        if [ "$arch" == "avx2" ]; then
            # Create a mapping file with '<old_sym> <suffixed_sym>' on each line.
            nm --defined-only --extern-only $tmp_obj | while read addr type sym ; do
              echo ${sym} ${sym}_${arch}
            done > renames.txt
            objcopy --redefine-syms=renames.txt $tmp_obj $dst_obj
        else
            mv $tmp_obj $dst_obj
        fi  
        to_link="$to_link $dst_obj"
    done
    rm -f libbitshuffle.a
    ar rs libbitshuffle.a $to_link
    mkdir -p $PREFIX/include/bitshuffle
    cp libbitshuffle.a $PREFIX/lib/
    cp $TP_SOURCE_DIR/$BITSHUFFLE_SOURCE/src/bitshuffle.h $PREFIX/include/bitshuffle/bitshuffle.h
    cp $TP_SOURCE_DIR/$BITSHUFFLE_SOURCE/src/bitshuffle_core.h $PREFIX/include/bitshuffle/bitshuffle_core.h
}

# croaring bitmap
build_croaringbitmap() {
    check_if_source_exist $CROARINGBITMAP_SOURCE
    cd $TP_SOURCE_DIR/CRoaring-0.2.60
    mkdir build -p && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    CXXFLAGS="-O3" \
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    $CMAKE_CMD -v -DROARING_BUILD_STATIC=ON -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INCLUDE_PATH="$TP_INSTALL_DIR/include" \
    -DENABLE_ROARING_TESTS=OFF \
    -DCMAKE_LIBRARY_PATH="$TP_INSTALL_DIR/lib;$TP_INSTALL_DIR/lib64" ..
    make -j$PARALLEL && make install
}
#orc
build_orc() {
    check_if_source_exist $ORC_SOURCE
    cd $TP_SOURCE_DIR/orc-1.5.8
    mkdir build -p && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    CXXFLAGS="-O3 -Wno-array-bounds" \
    $CMAKE_CMD ../ -DBUILD_JAVA=OFF \
    -DPROTOBUF_HOME=$TP_INSTALL_DIR \
    -DSNAPPY_HOME=$TP_INSTALL_DIR \
    -DGTEST_HOME=$TP_INSTALL_DIR \
    -DLZ4_HOME=$TP_INSTALL_DIR \
    -DLZ4_INCLUDE_DIR=$TP_INSTALL_DIR/include/lz4 \
    -DZLIB_HOME=$TP_INSTALL_DIR\
    -DBUILD_LIBHDFSPP=OFF \
    -DBUILD_CPP_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR 

    make -j$PARALLEL && make install
}

#cctz
build_cctz() {
    check_if_source_exist $CCTZ_SOURCE
    cd $TP_SOURCE_DIR/cctz-2.3
    export PREFIX=$TP_INSTALL_DIR
    make -j$PARALLEL && make install
}

# See https://github.com/apache/incubator-doris/issues/2910
# LLVM related codes have already be removed in master, so there is
# no need to build llvm tool here.
# Currently, some old release of Doris may still need it, so for now
# we just comment it, instead of remove it.
# build_llvm

build_libevent
build_zlib
build_lz4
build_bzip
build_lzo2
build_openssl
build_boost # must before thrift
build_protobuf
build_gflags
build_gtest
build_glog
build_rapidjson
build_snappy
build_gperftools
build_curl
build_re2
build_mysql
build_thrift
build_leveldb
build_brpc
build_rocksdb
build_librdkafka
build_flatbuffers
build_arrow
build_s2
build_bitshuffle
build_croaringbitmap
build_orc
build_cctz

echo "Finihsed to build all thirdparties"
