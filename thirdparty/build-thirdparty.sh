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

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     -j                 build thirdparty parallel
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'help' \
  -o 'j:' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

PARALLEL=$[$(nproc)/4+1]
if [[ $# -ne 1 ]] ; then
    while true; do
        case "$1" in
            -j) PARALLEL=$2; shift 2 ;;
            -h) HELP=1; shift ;;
            --help) HELP=1; shift ;;
            --) shift ;  break ;;
            *) echo "Internal error" ; exit 1 ;;
        esac
    done
fi

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

echo "Get params:
    PARALLEL            -- $PARALLEL
"

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
mkdir -p ${TP_DIR}/installed/lib64
pushd  ${TP_DIR}/installed/
ln -sf lib64 lib
popd

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


# Name of cmake build directory in each thirdpary project.
# Do not use `build`, because many projects contained a file named `BUILD`
# and if the filesystem is not case sensitive, `mkdir` will fail.
BUILD_DIR=doris_build

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
    make -j $PARALLEL && make install
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
    make -j $PARALLEL && make install_sw
    # NOTE(zc): remove this dynamic library files to make libcurl static link.
    # If I don't remove this files, I don't known how to make libcurl link static library
    if [ -f $TP_INSTALL_DIR/lib64/libcrypto.so ]; then
        rm -rf $TP_INSTALL_DIR/lib64/libcrypto.so*
    fi
    if [ -f $TP_INSTALL_DIR/lib64/libssl.so ]; then
        rm -rf $TP_INSTALL_DIR/lib64/libssl.so*
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

    make -j $PARALLEL && make install
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
    mkdir -p llvm-build && cd llvm-build
    rm -rf CMakeCache.txt CMakeFiles/
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    ${CMAKE_CMD} -G "${GENERATOR}" -DLLVM_REQUIRES_RTTI:Bool=True -DLLVM_TARGETS_TO_BUILD=${LLVM_TARGET} -DLLVM_ENABLE_TERMINFO=OFF LLVM_BUILD_LLVM_DYLIB:BOOL=OFF -DLLVM_ENABLE_PIC=true -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE="RELEASE" -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR/llvm ../$LLVM_SOURCE
    ${BUILD_SYSTEM} -j $PARALLEL REQUIRES_RTTI=1 && ${BUILD_SYSTEM} install
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
    make -j $PARALLEL && make install
}

# gflags
build_gflags() {
    check_if_source_exist $GFLAGS_SOURCE

    cd $TP_SOURCE_DIR/$GFLAGS_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    ${CMAKE_CMD} -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On ../
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
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
    make -j $PARALLEL && make install
}

# gtest
build_gtest() {
    check_if_source_exist $GTEST_SOURCE

    cd $TP_SOURCE_DIR/$GTEST_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    ${CMAKE_CMD} -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On ../
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# rapidjson
build_rapidjson() {
    check_if_source_exist $RAPIDJSON_SOURCE

    rm -rf $TP_INSTALL_DIR/rapidjson
    cp -r $TP_SOURCE_DIR/$RAPIDJSON_SOURCE/include/rapidjson $TP_INCLUDE_DIR/
}

# snappy
build_snappy() {
    check_if_source_exist $SNAPPY_SOURCE
    cd $TP_SOURCE_DIR/$SNAPPY_SOURCE

    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    CFLAGS="-O3" CXXFLAGS="-O3" ${CMAKE_CMD} -G "${GENERATOR}" -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On \
    -DCMAKE_INSTALL_INCLUDEDIR=$TP_INCLUDE_DIR/snappy \
    -DSNAPPY_BUILD_TESTS=0 ../
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install

    #build for libarrow.a
    cp $TP_INCLUDE_DIR/snappy/snappy-c.h  $TP_INCLUDE_DIR/snappy-c.h && \
    cp $TP_INCLUDE_DIR/snappy/snappy-sinksource.h  $TP_INCLUDE_DIR/snappy-sinksource.h && \
    cp $TP_INCLUDE_DIR/snappy/snappy-stubs-public.h  $TP_INCLUDE_DIR/snappy-stubs-public.h && \
    cp $TP_INCLUDE_DIR/snappy/snappy.h  $TP_INCLUDE_DIR/snappy.h
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
    make -j $PARALLEL && make install
}

# zlib
build_zlib() {
    check_if_source_exist $ZLIB_SOURCE
    cd $TP_SOURCE_DIR/$ZLIB_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR --static
    make -j $PARALLEL && make install

    # minizip
    cd contrib/minizip
    autoreconf --force --install
    ./configure --prefix=$TP_INSTALL_DIR --enable-static=yes --enable-shared=no
    make -j $PARALLEL && make install
}

# lz4
build_lz4() {
    check_if_source_exist $LZ4_SOURCE
    cd $TP_SOURCE_DIR/$LZ4_SOURCE

    make -j $PARALLEL install PREFIX=$TP_INSTALL_DIR \
    INCLUDEDIR=$TP_INCLUDE_DIR/lz4/
}

# bzip
build_bzip() {
    check_if_source_exist $BZIP_SOURCE
    cd $TP_SOURCE_DIR/$BZIP_SOURCE

    CFLAGS="-fPIC"
    make -j $PARALLEL install PREFIX=$TP_INSTALL_DIR
}

# lzo2
build_lzo2() {
    check_if_source_exist $LZO2_SOURCE
    cd $TP_SOURCE_DIR/$LZO2_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR} -fPIC" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR --disable-shared --enable-static
    make -j $PARALLEL && make install
}

# curl
build_curl() {
    check_if_source_exist $CURL_SOURCE
    cd $TP_SOURCE_DIR/$CURL_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
    LDFLAGS="-L${TP_LIB_DIR}" LIBS="-lcrypto -lssl -lcrypto -ldl" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR --disable-shared --enable-static \
    --without-librtmp --with-ssl=${TP_INSTALL_DIR} --without-libidn2 --disable-ldap --enable-ipv6 \
    --without-libssh2
    make -j $PARALLEL && make install
}

# re2
build_re2() {
    check_if_source_exist $RE2_SOURCE
    cd $TP_SOURCE_DIR/$RE2_SOURCE

    ${CMAKE_CMD} -G "${GENERATOR}" -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR
    ${BUILD_SYSTEM} -j $PARALLEL install
}

# boost
build_boost() {
    check_if_source_exist $BOOST_SOURCE
    cd $TP_SOURCE_DIR/$BOOST_SOURCE

    ./bootstrap.sh --prefix=$TP_INSTALL_DIR     
    ./b2 link=static runtime-link=static -j $PARALLEL --without-mpi --without-graph --without-graph_parallel --without-python cxxflags="-std=c++11 -g -fPIC -I$TP_INCLUDE_DIR -L$TP_LIB_DIR" install
}

# mysql
build_mysql() {
    check_if_source_exist $MYSQL_SOURCE
    check_if_source_exist $BOOST_SOURCE

    cd $TP_SOURCE_DIR/$MYSQL_SOURCE

    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    if [ ! -d $BOOST_SOURCE ]; then
        cp -rf $TP_SOURCE_DIR/$BOOST_SOURCE ./
    fi

    ${CMAKE_CMD} -G "${GENERATOR}" ../ -DWITH_BOOST=`pwd`/$BOOST_SOURCE -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR/mysql/ \
    -DCMAKE_INCLUDE_PATH=$TP_INCLUDE_DIR -DWITHOUT_SERVER=1 -DWITH_ZLIB=$TP_INSTALL_DIR \
    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O3 -g -fabi-version=2 -fno-omit-frame-pointer -fno-strict-aliasing -std=gnu++11" \
    -DDISABLE_SHARED=1 -DBUILD_SHARED_LIBS=0 -DZLIB_LIBRARY=$TP_INSTALL_DIR/lib/libz.a
    ${BUILD_SYSTEM} -j $PARALLEL mysqlclient

    # copy headers manually
    rm -rf ../../../installed/include/mysql/
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
    rm -rf out-shared/ out-static/
    CXXFLAGS="-fPIC" make -j $PARALLEL
    cp out-static/libleveldb.a ../../installed/lib/libleveldb.a
    cp -r include/leveldb ../../installed/include/
}

# brpc
build_brpc() {
    check_if_source_exist $BRPC_SOURCE

    cd $TP_SOURCE_DIR/$BRPC_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    ${CMAKE_CMD} -G "${GENERATOR}" -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DBRPC_WITH_GLOG=ON -DWITH_GLOG=ON -DGFLAGS_LIBRARY=$TP_INSTALL_DIR/lib/libgflags.a -DGLOG_LIB=$TP_INSTALL_DIR/lib \
    -DGFLAGS_INCLUDE_DIR=$TP_INSTALL_DIR/include -DGLOG_LIB=$TP_INSTALL_DIR/lib/libglog.a -DCMAKE_INCLUDE_PATH="$TP_INSTALL_DIR/include" \
    -DPROTOBUF_PROTOC_EXECUTABLE=$TP_INSTALL_DIR/bin/protoc .. 
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# rocksdb
build_rocksdb() {
    check_if_source_exist $ROCKSDB_SOURCE

    cd $TP_SOURCE_DIR/$ROCKSDB_SOURCE

    CFLAGS="-I ${TP_INCLUDE_DIR} -I ${TP_INCLUDE_DIR}/snappy -I ${TP_INCLUDE_DIR}/lz4" CXXFLAGS="-fPIC -Wno-deprecated-copy -Wno-stringop-truncation -Wno-pessimizing-move" LDFLAGS="-static-libstdc++ -static-libgcc" \
        PORTABLE=1 make USE_RTTI=1 -j $PARALLEL static_lib
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
    make -j $PARALLEL && make install
}

# libunixodbc
build_libunixodbc() {
    check_if_source_exist $ODBC_SOURCE

    cd $TP_SOURCE_DIR/$ODBC_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
    LDFLAGS="-L${TP_LIB_DIR}" \
    CFLAGS="-fPIC" \
    ./configure --prefix=$TP_INSTALL_DIR --with-included-ltdl --enable-static=yes --enable-shared=no 
    make -j $PARALLEL && make install
}
  
# flatbuffers
build_flatbuffers() {
  check_if_source_exist $FLATBUFFERS_SOURCE
  cd $TP_SOURCE_DIR/$FLATBUFFERS_SOURCE
  mkdir -p $BUILD_DIR && cd $BUILD_DIR
  rm -rf CMakeCache.txt CMakeFiles/
  CXXFLAGS="-fPIC -Wno-class-memaccess" \
  LDFLAGS="-static-libstdc++ -static-libgcc" \
  ${CMAKE_CMD} -G "${GENERATOR}" ..
  ${BUILD_SYSTEM} -j $PARALLEL
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
    export ARROW_Thrift_URL=${TP_SOURCE_DIR}/${THRIFT_NAME}
    export LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc"

    #--trace-expand \
    ${CMAKE_CMD} -G "${GENERATOR}" -DARROW_PARQUET=ON -DARROW_IPC=ON -DARROW_USE_GLOG=off \
    -DARROW_BUILD_SHARED=OFF -DARROW_BUILD_STATIC=ON -DARROW_WITH_ZSTD=ON \
    -DLZ4_INCLUDE_DIR=$TP_INSTALL_DIR \
    -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INSTALL_LIBDIR=lib64 \
    -DARROW_BOOST_USE_SHARED=OFF -DARROW_GFLAGS_USE_SHARED=OFF \
    -DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$TP_INSTALL_DIR \
    -DPARQUET_ARROW_LINKAGE=static \
    -Dgflags_ROOT=$TP_INSTALL_DIR/ \
    -DSnappy_ROOT=$TP_INSTALL_DIR/ \
    -DGLOG_ROOT=$TP_INSTALL_DIR/ \
    -DGFLAGS_LIBRARY=$TP_INSTALL_DIR/lib/libgflags.a \
    -DGLOG_LIB=$TP_INSTALL_DIR/lib/libglog.a \
    -DLZ4_ROOT=$TP_INSTALL_DIR/ \
    -DLZ4_INCLUDE_DIR=$TP_INSTALL_DIR/include/lz4 \
    -DZSTD_SOURCE=BUNDLED \
    -DZLIB_LIBRARY=$TP_INSTALL_DIR/lib/libz.a -DZLIB_INCLUDE_DIR=$TP_INSTALL_DIR/include \
    -DGFLAGS_LIBRARY=$TP_INSTALL_DIR/lib/libgflags.a \
    -Ddouble-conversion_SOURCE=BUNDLED \
    -DThrift_ROOT=$TP_INSTALL_DIR/ ..

    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install

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
    cd $TP_SOURCE_DIR/$S2_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    CXXFLAGS="-O3" \
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    ${CMAKE_CMD} -G "${GENERATOR}" -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INCLUDE_PATH="$TP_INSTALL_DIR/include" \
    -DBUILD_SHARED_LIBS=OFF \
    -DGFLAGS_ROOT_DIR="$TP_INSTALL_DIR/include" \
    -DWITH_GFLAGS=ON \
    -DGLOG_ROOT_DIR="$TP_INSTALL_DIR/include" \
    -DCMAKE_LIBRARY_PATH="$TP_INSTALL_DIR/lib" \
    -DOPENSSL_ROOT_DIR=$TP_INSTALL_DIR/include \
    -DWITH_GLOG=ON ..
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
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
        ${CC:-$DORIS_GCC_HOME/bin/gcc} $EXTRA_CFLAGS $arch_flag -std=c99 -I$PREFIX/include/lz4/ -O3 -DNDEBUG -fPIC -c \
            "src/bitshuffle_core.c" \
            "src/bitshuffle.c" \
            "src/iochain.c"
        # Merge the object files together to produce a combined .o file.
        $DORIS_BIN_UTILS/ld -r -o $tmp_obj bitshuffle_core.o bitshuffle.o iochain.o
        # For the AVX2 symbols, suffix them.
        if [ "$arch" == "avx2" ]; then
            # Create a mapping file with '<old_sym> <suffixed_sym>' on each line.
            $DORIS_BIN_UTILS/nm --defined-only --extern-only $tmp_obj | while read addr type sym ; do
              echo ${sym} ${sym}_${arch}
            done > renames.txt
            $DORIS_BIN_UTILS/objcopy --redefine-syms=renames.txt $tmp_obj $dst_obj
        else
            mv $tmp_obj $dst_obj
        fi  
        to_link="$to_link $dst_obj"
    done
    rm -f libbitshuffle.a
    $DORIS_BIN_UTILS/ar rs libbitshuffle.a $to_link
    mkdir -p $PREFIX/include/bitshuffle
    cp libbitshuffle.a $PREFIX/lib/
    cp $TP_SOURCE_DIR/$BITSHUFFLE_SOURCE/src/bitshuffle.h $PREFIX/include/bitshuffle/bitshuffle.h
    cp $TP_SOURCE_DIR/$BITSHUFFLE_SOURCE/src/bitshuffle_core.h $PREFIX/include/bitshuffle/bitshuffle_core.h
}

# croaring bitmap
build_croaringbitmap() {
    check_if_source_exist $CROARINGBITMAP_SOURCE
    cd $TP_SOURCE_DIR/$CROARINGBITMAP_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    CXXFLAGS="-O3" \
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    ${CMAKE_CMD} -G "${GENERATOR}" -DROARING_BUILD_STATIC=ON -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INCLUDE_PATH="$TP_INSTALL_DIR/include" \
    -DENABLE_ROARING_TESTS=OFF ..
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# fmt
build_fmt() {
    check_if_source_exist $FMT_SOURCE
    cd $TP_SOURCE_DIR/$FMT_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD -G "${GENERATOR}" -DBUILD_SHARED_LIBS=FALSE -DFMT_TEST=OFF -DFMT_DOC=OFF -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR ..
    ${BUILD_SYSTEM} -j$PARALLEL && ${BUILD_SYSTEM} install
}

# parallel_hashmap
build_parallel_hashmap() {
    check_if_source_exist $PARALLEL_HASHMAP_SOURCE
    cd $TP_SOURCE_DIR/$PARALLEL_HASHMAP_SOURCE
    cp -r parallel_hashmap $TP_INSTALL_DIR/include/
}

# pdqsort
build_pdqsort() {
    check_if_source_exist $PDQSORT_SOURCE
    cd $TP_SOURCE_DIR/$PDQSORT_SOURCE
    cp -r pdqsort.h $TP_INSTALL_DIR/include/
}

# libdivide
build_libdivide() {
    check_if_source_exist $LIBDIVIDE_SOURCE
    cd $TP_SOURCE_DIR/$LIBDIVIDE_SOURCE
    cp -r libdivide.h $TP_INSTALL_DIR/include/
}

#orc
build_orc() {
    check_if_source_exist $ORC_SOURCE
    cd $TP_SOURCE_DIR/$ORC_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    CXXFLAGS="-O3 -Wno-array-bounds" \
    ${CMAKE_CMD} -G "${GENERATOR}" ../ -DBUILD_JAVA=OFF \
    -DPROTOBUF_HOME=$TP_INSTALL_DIR \
    -DSNAPPY_HOME=$TP_INSTALL_DIR \
    -DGTEST_HOME=$TP_INSTALL_DIR \
    -DLZ4_HOME=$TP_INSTALL_DIR \
    -DLZ4_INCLUDE_DIR=$TP_INSTALL_DIR/include/lz4 \
    -DZLIB_HOME=$TP_INSTALL_DIR\
    -DBUILD_LIBHDFSPP=OFF \
    -DBUILD_CPP_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR 

    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

#cctz
build_cctz() {
    check_if_source_exist $CCTZ_SOURCE
    cd $TP_SOURCE_DIR/$CCTZ_SOURCE
    export PREFIX=$TP_INSTALL_DIR
    make -j $PARALLEL && make install
}

# all js and csss related
build_js_and_css() {
    check_if_source_exist $DATATABLES_SOURCE
    check_if_source_exist Bootstrap-3.3.7/
    check_if_source_exist jQuery-3.3.1/

    mkdir -p $TP_INSTALL_DIR/webroot/
    cd $TP_SOURCE_DIR/
    cp -r $DATATABLES_SOURCE $TP_INSTALL_DIR/webroot/
    cp -r Bootstrap-3.3.7/ $TP_INSTALL_DIR/webroot/
    cp -r jQuery-3.3.1/ $TP_INSTALL_DIR/webroot/
    cp bootstrap-table.min.js $TP_INSTALL_DIR/webroot/Bootstrap-3.3.7/js
    cp bootstrap-table.min.css $TP_INSTALL_DIR/webroot/Bootstrap-3.3.7/css
}

build_tsan_header() {
    cd $TP_SOURCE_DIR/
    if [[ ! -f $TSAN_HEADER_FILE ]]; then
        echo "$TSAN_HEADER_FILE should exist."
        exit 1
    fi

    mkdir -p $TP_INSTALL_DIR/include/sanitizer/
    cp $TSAN_HEADER_FILE $TP_INSTALL_DIR/include/sanitizer/
}

# aws-c-common
build_aws_c_common() {
    check_if_source_exist $AWS_C_COMMON_SOURCE
    cd $TP_SOURCE_DIR/$AWS_C_COMMON_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    cmake -G "${GENERATOR}" .. -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR -DCMAKE_PREFIX_PATH=$TP_INSTALL_DIR -DBUILD_SHARED_LIBS=OFF
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# aws-c-event-stream
build_aws_c_event_stream() {
    check_if_source_exist $AWS_C_EVENT_STREAM_SOURCE
    cd $TP_SOURCE_DIR/$AWS_C_EVENT_STREAM_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    cmake -G "${GENERATOR}" .. -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR  -DCMAKE_PREFIX_PATH=$TP_INSTALL_DIR -DBUILD_SHARED_LIBS=OFF \
    -DBUILD_TESTING=OFF
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# aws-checksums
build_aws_checksums() {
    check_if_source_exist $AWS_CHECKSUMS_SOURCE
    cd $TP_SOURCE_DIR/$AWS_CHECKSUMS_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    cmake -G "${GENERATOR}" .. -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR -DCMAKE_PREFIX_PATH=$TP_INSTALL_DIR -DBUILD_SHARED_LIBS=OFF
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# aws-c-io
build_aws_c_io() {
    check_if_source_exist $AWS_C_IO_SOURCE
    cd $TP_SOURCE_DIR/$AWS_C_IO_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    cmake -G "${GENERATOR}" .. -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR -DCMAKE_PREFIX_PATH=$TP_INSTALL_DIR -DBUILD_SHARED_LIBS=OFF
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# aws-s2n
build_aws_s2n() {
    check_if_source_exist $AWS_S2N_SOURCE
    cd $TP_SOURCE_DIR/$AWS_S2N_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    cmake -G "${GENERATOR}" .. -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR -DCMAKE_PREFIX_PATH=$TP_INSTALL_DIR -DBUILD_SHARED_LIBS=OFF
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# aws-c-cal
build_aws_c_cal() {
    check_if_source_exist $AWS_C_CAL_SOURCE
    cd $TP_SOURCE_DIR/$AWS_C_CAL_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    cmake -G "${GENERATOR}" .. -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR -DCMAKE_PREFIX_PATH=$TP_INSTALL_DIR -DBUILD_SHARED_LIBS=OFF
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# aws_sdk
build_aws_sdk() {
    check_if_source_exist $AWS_SDK_SOURCE
    cd $TP_SOURCE_DIR/$AWS_SDK_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    $CMAKE_CMD -G "${GENERATOR}" .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DBUILD_DEPS=OFF -DCMAKE_PREFIX_PATH=$TP_INSTALL_DIR -DBUILD_SHARED_LIBS=OFF -DENABLE_TESTING=OFF \
    -DCMAKE_MODULE_PATH=$TP_INSTALL_DIR/lib64/cmake -DBUILD_ONLY="s3"
    ${BUILD_SYSTEM} -j $PARALLEL && ${BUILD_SYSTEM} install
}

# lzma
build_lzma() {
    check_if_source_exist $LZMA_SOURCE
    cd $TP_SOURCE_DIR/$LZMA_SOURCE
    export ACLOCAL_PATH=/usr/share/aclocal
    sh autogen.sh
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    ../configure --prefix=$TP_INSTALL_DIR --enable-shared=no --with-pic
    make -j $PARALLEL && make install
}

# xml2
build_xml2() {
    check_if_source_exist $XML2_SOURCE
    cd $TP_SOURCE_DIR/$XML2_SOURCE
    export ACLOCAL_PATH=/usr/share/aclocal
    sh autogen.sh
    make distclean
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    ../configure --prefix=$TP_INSTALL_DIR --enable-shared=no --with-pic --with-python=no
    make -j $PARALLEL && make install
}

# gsasl
build_gsasl() {
    check_if_source_exist $GSASL_SOURCE
    cd $TP_SOURCE_DIR/$GSASL_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    ../configure --prefix=$TP_INSTALL_DIR --enable-shared=no --with-pic
    make -j $PARALLEL && make install
}

# hdfs3
build_hdfs3() {
    check_if_source_exist $HDFS3_SOURCE
    cd $TP_SOURCE_DIR/$HDFS3_SOURCE
    mkdir -p $BUILD_DIR && cd $BUILD_DIR
    # export CC=/opt/compiler/gcc-10/bin/gcc
    # export CXX=/opt/compiler/gcc-10/bin/g++
    ../bootstrap --dependency=$TP_INSTALL_DIR --prefix=$TP_INSTALL_DIR
    make -j $PARALLEL && make install
}

# See https://github.com/apache/incubator-doris/issues/2910
# LLVM related codes have already be removed in master, so there is
# no need to build llvm tool here.
# Currently, some old release of Doris may still need it, so for now
# we just comment it, instead of remove it.
# build_llvm

build_libunixodbc
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
build_fmt
build_parallel_hashmap
build_pdqosrt
build_libdivide
build_orc
build_cctz
build_tsan_header
build_mysql
build_aws_c_common
build_aws_s2n
build_aws_c_cal
build_aws_c_io
build_aws_checksums
build_aws_c_event_stream
build_aws_sdk
build_js_and_css
build_lzma
build_xml2
build_gsasl
build_hdfs3

echo "Finished to build all thirdparties"
