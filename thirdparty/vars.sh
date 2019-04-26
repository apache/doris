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
LIBEVENT_DOWNLOAD="https://doris-incubating-repo.bj.bcebos.com/thirdparty/libevent-20180622-24236aed01798303745470e6c498bf606e88724a.zip"
LIBEVENT_NAME=libevent-20180622-24236aed01798303745470e6c498bf606e88724a.zip
LIBEVENT_SOURCE=libevent-master
LIBEVENT_MD5SUM="e8b9ba50270ba3b520aec8ff1089f9d7"

# openssl
OPENSSL_DOWNLOAD="https://www.openssl.org/source/openssl-1.0.2k.tar.gz"
OPENSSL_NAME=openssl-1.0.2k.tar.gz
OPENSSL_SOURCE=openssl-1.0.2k
OPENSSL_MD5SUM="f965fc0bf01bf882b31314b61391ae65"

# thrift
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz"
THRIFT_NAME=thrift-0.9.3.tar.gz
THRIFT_SOURCE=thrift-0.9.3
THRIFT_MD5SUM="88d667a8ae870d5adeca8cb7d6795442"

# llvm
LLVM_DOWNLOAD="http://releases.llvm.org/3.4.2/llvm-3.4.2.src.tar.gz"
LLVM_NAME=llvm-3.4.2.src.tar.gz
LLVM_SOURCE=llvm-3.4.2.src
LLVM_MD5SUM="a20669f75967440de949ac3b1bad439c"

# clang
CLANG_DOWNLOAD="http://releases.llvm.org/3.4.2/cfe-3.4.2.src.tar.gz"
CLANG_NAME=cfe-3.4.2.src.tar.gz
CLANG_SOURCE=cfe-3.4.2.src
CLANG_MD5SUM="87945973b7c73038871c5f849a818588"

# compiler-rt
COMPILER_RT_DOWNLOAD="http://releases.llvm.org/5.0.0/compiler-rt-5.0.0.src.tar.xz"
COMPILER_RT_NAME=compiler-rt-5.0.0.src.tar.xz
COMPILER_RT_SOURCE=compiler-rt-5.0.0.src
COMPILER_RT_MD5SUM="da735894133589cbc6052c8ef06b1230"

# protobuf
PROTOBUF_DOWNLOAD="https://github.com/google/protobuf/archive/v3.5.1.tar.gz"
PROTOBUF_NAME=protobuf-3.5.1.tar.gz
PROTOBUF_SOURCE=protobuf-3.5.1
PROTOBUF_MD5SUM="710f1a75983092c9b45ecef207236104"

# gflags
GFLAGS_DOWNLOAD="https://github.com/gflags/gflags/archive/v2.2.0.tar.gz"
GFLAGS_NAME=gflags-2.2.0.tar.gz
GFLAGS_SOURCE=gflags-2.2.0
GFLAGS_MD5SUM="b99048d9ab82d8c56e876fb1456c285e"

# glog
GLOG_DOWNLOAD="https://github.com/google/glog/archive/v0.3.3.tar.gz"
GLOG_NAME=glog-0.3.3.tar.gz
GLOG_SOURCE=glog-0.3.3
GLOG_MD5SUM="c1f86af27bd9c73186730aa957607ed0"

# gtest
GTEST_DOWNLOAD="https://github.com/google/googletest/archive/release-1.8.0.tar.gz"
GTEST_NAME=googletest-release-1.8.0.tar.gz
GTEST_SOURCE=googletest-release-1.8.0
GTEST_MD5SUM="16877098823401d1bf2ed7891d7dce36"

# snappy
SNAPPY_DOWNLOAD="https://github.com/google/snappy/archive/1.1.7.tar.gz"
SNAPPY_NAME=snappy-1.1.7.tar.gz
SNAPPY_SOURCE=snappy-1.1.7
SNAPPY_MD5SUM="ee9086291c9ae8deb4dac5e0b85bf54a"

# gperftools
GPERFTOOLS_DOWNLOAD="https://github.com/gperftools/gperftools/archive/gperftools-2.7.tar.gz"
GPERFTOOLS_NAME=gperftools-2.7.tar.gz
GPERFTOOLS_SOURCE=gperftools-gperftools-2.7
GPERFTOOLS_MD5SUM="797e7b7f6663288e2b90ab664861c61a"

# zlib
ZLIB_DOWNLOAD="https://sourceforge.net/projects/libpng/files/zlib/1.2.11/zlib-1.2.11.tar.gz"
ZLIB_NAME=zlib-1.2.11.tar.gz
ZLIB_SOURCE=zlib-1.2.11 
ZLIB_MD5SUM="1c9f62f0778697a09d36121ead88e08e"

# lz4
LZ4_DOWNLOAD="https://github.com/lz4/lz4/archive/v1.7.5.tar.gz"
LZ4_NAME=lz4-1.7.5.tar.gz
LZ4_SOURCE=lz4-1.7.5
LZ4_MD5SUM="c9610c5ce97eb431dddddf0073d919b9"

# bzip
# BZIP_DOWNLOAD="http://www.bzip.org/1.0.6/bzip2-1.0.6.tar.gz"
BZIP_DOWNLOAD="https://fossies.org/linux/misc/bzip2-1.0.6.tar.gz"
BZIP_NAME=bzip2-1.0.6.tar.gz
BZIP_SOURCE=bzip2-1.0.6
BZIP_MD5SUM="00b516f4704d4a7cb50a1d97e6e8e15b"

# lzo2
LZO2_DOWNLOAD="http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz"
LZO2_NAME=lzo-2.10.tar.gz
LZO2_SOURCE=lzo-2.10
LZO2_MD5SUM="39d3f3f9c55c87b1e5d6888e1420f4b5"

# rapidjson
RAPIDJSON_DOWNLOAD="https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz"
RAPIDJSON_NAME=rapidjson-1.1.0.tar.gz
RAPIDJSON_SOURCE=rapidjson-1.1.0
RAPIDJSON_MD5SUM="badd12c511e081fec6c89c43a7027bce"

# curl
CURL_DOWNLOAD="https://curl.haxx.se/download/curl-7.54.1.tar.gz"
CURL_NAME=curl-7.54.1.tar.gz
CURL_SOURCE=curl-7.54.1
CURL_MD5SUM="21a6e5658fd55103a90b11de7b2a8a8c"

# RE2
RE2_DOWNLOAD="https://github.com/google/re2/archive/2017-05-01.tar.gz"
RE2_NAME=re2-2017-05-01.tar.gz
RE2_SOURCE=re2-2017-05-01
RE2_MD5SUM="4aa65a0b22edacb7ddcd7e4aec038dcf"

# boost
BOOST_DOWNLOAD="https://dl.bintray.com/boostorg/release/1.64.0/source/boost_1_64_0.tar.gz"
BOOST_NAME=boost_1_64_0.tar.gz
BOOST_SOURCE=boost_1_64_0
BOOST_MD5SUM="319c6ffbbeccc366f14bb68767a6db79"

# mysql
MYSQL_DOWNLOAD="https://github.com/mysql/mysql-server/archive/mysql-5.7.18.tar.gz"
MYSQL_NAME=mysql-5.7.18.tar.gz
MYSQL_SOURCE=mysql-server-mysql-5.7.18
MYSQL_MD5SUM="58598b10dce180e4d1fbdd7cf5fa68d6"

# boost for mysql
BOOST_FOR_MYSQL_DOWNLOAD="http://sourceforge.net/projects/boost/files/boost/1.59.0/boost_1_59_0.tar.gz"
BOOST_FOR_MYSQL_NAME=boost_1_59_0.tar.gz
BOOST_FOR_MYSQL_SOURCE=boost_1_59_0
BOOST_FOR_MYSQL_MD5SUM="51528a0e3b33d9e10aaa311d9eb451e3"

# leveldb
LEVELDB_DOWNLOAD="https://github.com/google/leveldb/archive/v1.20.tar.gz"
LEVELDB_NAME=leveldb-1.20.tar.gz
LEVELDB_SOURCE=leveldb-1.20
LEVELDB_MD5SUM="298b5bddf12c675d6345784261302252"

# brpc
BRPC_DOWNLOAD="https://github.com/brpc/brpc/archive/v0.9.0.tar.gz"
BRPC_NAME=brpc-0.9.0.tar.gz
BRPC_SOURCE=incubator-brpc-0.9.0
BRPC_MD5SUM="79dfdc8b6e2d7a08dc68f14c5fabe6b7"

# rocksdb
ROCKSDB_DOWNLOAD="https://github.com/facebook/rocksdb/archive/v5.14.2.tar.gz"
ROCKSDB_NAME=rocksdb-5.14.2.tar.gz
ROCKSDB_SOURCE=rocksdb-5.14.2
ROCKSDB_MD5SUM="b72720ea3b1e9ca9e4ed0febfef65b14"

# librdkafka
LIBRDKAFKA_DOWNLOAD="https://github.com/edenhill/librdkafka/archive/v0.11.6-RC5.tar.gz"
LIBRDKAFKA_NAME=librdkafka-0.11.6-RC5.tar.gz
LIBRDKAFKA_SOURCE=librdkafka-0.11.6-RC5
LIBRDKAFKA_MD5SUM="2e4ecef2df277e55a0144eb6d185e18a"

# all thirdparties which need to be downloaded is set in array TP_ARCHIVES
export TP_ARCHIVES="LIBEVENT OPENSSL THRIFT LLVM CLANG COMPILER_RT PROTOBUF GFLAGS GLOG GTEST RAPIDJSON SNAPPY GPERFTOOLS ZLIB LZ4 BZIP LZO2 CURL RE2 BOOST MYSQL BOOST_FOR_MYSQL LEVELDB BRPC ROCKSDB LIBRDKAFKA"
