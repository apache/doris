#!/bin/bash
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

############################################################
# You may have to set variables bellow,
# which are used for compiling thirdparties and palo itself.
############################################################

# --job param for *make*
PARALLEL=$[$(nproc)/4+1]

###################################################
# DO NOT change variables bellow unless you known 
# what you are doing.
###################################################

# thirdparties will be downloaded and unpacked here
export TP_SOURCE_DIR=$TP_DIR/src

# thirdparties will be installed to here
export TP_INSTALL_DIR=$TP_DIR/installed

# patches for all thirdparties
export TP_PATCH_DIR=$TP_DIR/patches

# header files of all thirdparties will be intalled to here
export TP_INCLUDE_DIR=$TP_INSTALL_DIR/include

# libraries of all thirdparties will be intalled to here
export TP_LIB_DIR=$TP_INSTALL_DIR/lib

# all java libraries will be unpacked to here
export TP_JAR_DIR=$TP_INSTALL_DIR/lib/jar

#####################################################
# Download url, filename and unpaced filename
# of all thirdparties
#####################################################

# libevent
# the last release version of libevent is 2.1.8, which was released on 26 Jan 2017, that is too old.
# so we use the master version of libevent, which is downloaded on 22 Jun 2018, with commit 24236aed01798303745470e6c498bf606e88724a
LIBEVENT_DOWNLOAD="http://palo-opensource.gz.bcebos.com/libevent-20180622-24236aed01798303745470e6c498bf606e88724a.zip?authorization=bce-auth-v1%2F069fc2786e464e63a5f1183824ddb522%2F2018-06-22T02%3A37%3A48Z%2F-1%2Fhost%2F031b0cc42ab83ca4e0ec3608cba963e95c1ddc46fc70a14457323e2d7960e6ef"
LIBEVENT_NAME=libevent-20180622-24236aed01798303745470e6c498bf606e88724a.zip
LIBEVENT_SOURCE=libevent-master

# openssl
OPENSSL_DOWNLOAD="https://www.openssl.org/source/openssl-1.0.2k.tar.gz"
OPENSSL_NAME=openssl-1.0.2k.tar.gz
OPENSSL_SOURCE=openssl-1.0.2k

# thrift
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz"
THRIFT_NAME=thrift-0.9.3.tar.gz
THRIFT_SOURCE=thrift-0.9.3

# llvm
LLVM_DOWNLOAD="http://releases.llvm.org/3.4.2/llvm-3.4.2.src.tar.gz"
LLVM_NAME=llvm-3.4.2.src.tar.gz
LLVM_SOURCE=llvm-3.4.2.src

# clang
CLANG_DOWNLOAD="http://releases.llvm.org/3.4.2/cfe-3.4.2.src.tar.gz"
CLANG_NAME=cfe-3.4.2.src.tar.gz
CLANG_SOURCE=cfe-3.4.2.src

# compiler-rt
COMPILER_RT_DOWNLOAD="http://releases.llvm.org/5.0.0/compiler-rt-5.0.0.src.tar.xz"
COMPILER_RT_NAME=compiler-rt-5.0.0.src.tar.xz
COMPILER_RT_SOURCE=compiler-rt-5.0.0.src

# protobuf
PROTOBUF_DOWNLOAD="https://github.com/google/protobuf/archive/v3.5.1.tar.gz"
PROTOBUF_NAME=protobuf-3.5.1.tar.gz
PROTOBUF_SOURCE=protobuf-3.5.1

# gflags
GFLAGS_DOWNLOAD="https://github.com/gflags/gflags/archive/v2.2.0.tar.gz"
GFLAGS_NAME=gflags-2.2.0.tar.gz
GFLAGS_SOURCE=gflags-2.2.0

# glog
GLOG_DOWNLOAD="https://github.com/google/glog/archive/v0.3.3.tar.gz"
GLOG_NAME=glog-0.3.3.tar.gz
GLOG_SOURCE=glog-0.3.3

# gtest
GTEST_DOWNLOAD="https://github.com/google/googletest/archive/release-1.8.0.tar.gz"
GTEST_NAME=googletest-release-1.8.0.tar.gz
GTEST_SOURCE=googletest-release-1.8.0

# snappy
SNAPPY_DOWNLOAD="https://github.com/google/snappy/releases/download/1.1.4/snappy-1.1.4.tar.gz"
SNAPPY_NAME=snappy-1.1.4.tar.gz
SNAPPY_SOURCE=snappy-1.1.4

# gperftools
GPERFTOOLS_DOWNLOAD="https://github.com/gperftools/gperftools/archive/gperftools-2.7.tar.gz"
GPERFTOOLS_NAME=gperftools-2.7.tar.gz
GPERFTOOLS_SOURCE=gperftools-gperftools-2.7

# zlib
ZLIB_DOWNLOAD="https://sourceforge.net/projects/libpng/files/zlib/1.2.11/zlib-1.2.11.tar.gz"
ZLIB_NAME=zlib-1.2.11.tar.gz
ZLIB_SOURCE=zlib-1.2.11 

# lz4
LZ4_DOWNLOAD="https://github.com/lz4/lz4/archive/v1.7.5.tar.gz"
LZ4_NAME=lz4-1.7.5.tar.gz
LZ4_SOURCE=lz4-1.7.5

# bzip
# BZIP_DOWNLOAD="http://www.bzip.org/1.0.6/bzip2-1.0.6.tar.gz"
BZIP_DOWNLOAD="https://fossies.org/linux/misc/bzip2-1.0.6.tar.gz"
BZIP_NAME=bzip2-1.0.6.tar.gz
BZIP_SOURCE=bzip2-1.0.6

# lzo2
LZO2_DOWNLOAD="https://github.com/damageboy/lzo2/archive/master.zip"
LZO2_NAME=lzo2-master.zip
LZO2_SOURCE=lzo2-master

# rapidjson
RAPIDJSON_DOWNLOAD="https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz"
RAPIDJSON_NAME=rapidjson-1.1.0.tar.gz
RAPIDJSON_SOURCE=rapidjson-1.1.0

# curl
CURL_DOWNLOAD="https://curl.haxx.se/download/curl-7.54.0.tar.gz"
CURL_NAME=curl-7.54.0.tar.gz
CURL_SOURCE=curl-7.54.0

# RE2
RE2_DOWNLOAD="https://github.com/google/re2/archive/2017-05-01.tar.gz"
RE2_NAME=re2-2017-05-01.tar.gz
RE2_SOURCE=re2-2017-05-01

# boost
BOOST_DOWNLOAD="https://dl.bintray.com/boostorg/release/1.64.0/source/boost_1_64_0.tar.gz"
BOOST_NAME=boost_1_64_0.tar.gz
BOOST_SOURCE=boost_1_64_0

# mysql
MYSQL_DOWNLOAD="https://github.com/mysql/mysql-server/archive/mysql-5.7.18.tar.gz"
MYSQL_NAME=mysql-5.7.18.tar.gz
MYSQL_SOURCE=mysql-server-mysql-5.7.18

# boost for mysql
BOOST_FOR_MYSQL_DOWNLOAD="http://sourceforge.net/projects/boost/files/boost/1.59.0/boost_1_59_0.tar.gz"
BOOST_FOR_MYSQL_NAME=boost_1_59_0.tar.gz
BOOST_FOR_MYSQL_SOURCE=boost_1_59_0

# leveldb
LEVELDB_DOWNLOAD="https://github.com/google/leveldb/archive/v1.20.tar.gz"
LEVELDB_NAME=leveldb-1.20.tar.gz
LEVELDB_SOURCE=leveldb-1.20

# brpc
BRPC_DOWNLOAD="https://github.com/brpc/brpc/archive/v0.9.0.tar.gz"
BRPC_NAME=brpc-0.9.0.tar.gz
BRPC_SOURCE=brpc-0.9.0

# JDK
JDK_DOWNLOAD="http://mirror.cnop.net/jdk/linux/jdk-8u131-linux-x64.tar.gz"
JDK_NAME=jdk-8u131-linux-x64.tar.gz
JDK_SOURCE=jdk1.8.0_131

# rocksdb
ROCKSDB_DOWNLOAD="https://github.com/facebook/rocksdb/archive/v5.14.2.tar.gz"
ROCKSDB_NAME=rocksdb-5.14.2.tar.gz
ROCKSDB_SOURCE=rocksdb-5.14.2

# librdkafka
LIBRDKAFKA_DOWNLOAD="https://github.com/edenhill/librdkafka/archive/v0.11.6-RC5.tar.gz"
LIBRDKAFKA_NAME=librdkafka-0.11.6-RC5.tar.gz
LIBRDKAFKA_SOURCE=librdkafka-0.11.6-RC5

# all thirdparties which need to be downloaded is set in array TP_ARCHIVES
export TP_ARCHIVES="LIBEVENT OPENSSL THRIFT LLVM CLANG COMPILER_RT PROTOBUF GFLAGS GLOG GTEST RAPIDJSON SNAPPY GPERFTOOLS ZLIB LZ4 BZIP LZO2 CURL RE2 BOOST MYSQL BOOST_FOR_MYSQL LEVELDB BRPC JDK ROCKSDB LIBRDKAFKA"
