#!/bin/bash

# Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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
PARALLEL=4

###################################################
# DO NOT change variables bellow unless you known 
# what you are doing.
###################################################

# thirdparty root dir. default is where this script is.
export TP_DIR=$PALO_HOME/thirdparty

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
LIBEVENT_DOWNLOAD="https://github.com/libevent/libevent/releases/download/release-2.1.8-stable/libevent-2.1.8-stable.tar.gz"
LIBEVENT_NAME=libevent-2.1.8-stable.tar.gz 
LIBEVENT_SOURCE=libevent-2.1.8-stable

# openssl
OPENSSL_DOWNLOAD="https://www.openssl.org/source/openssl-1.0.2k.tar.gz"
OPENSSL_NAME=openssl-1.0.2k.tar.gz
OPENSSL_SOURCE=openssl-1.0.2k

# thrift
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz"
THRIFT_NAME=thrift-0.9.3.tar.gz
THRIFT_SOURCE=thrift-0.9.3

# llvm
LLVM_DOWNLOAD="http://releases.llvm.org/3.3/llvm-3.3.src.tar.gz"
LLVM_NAME=llvm-3.3.src.tar.gz
LLVM_SOURCE=llvm-3.3.src

# clang
CLANG_DOWNLOAD="http://releases.llvm.org/3.3/cfe-3.3.src.tar.gz"
CLANG_NAME=cfe-3.3.src.tar.gz
CLANG_SOURCE=cfe-3.3.src

# compiler-rt
COMPILER_RT_DOWNLOAD="http://releases.llvm.org/3.3/compiler-rt-3.3.src.tar.gz"
COMPILER_RT_NAME=compiler-rt-3.3.src.tar.gz
COMPILER_RT_SOURCE=compiler-rt-3.3.src

# protobuf
PROTOBUF_DOWNLOAD="https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz"
PROTOBUF_NAME=protobuf-2.6.1.tar.gz
PROTOBUF_SOURCE=protobuf-2.6.1

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

# libunwind
LIBUNWIND_DOWNLOAD="http://download.savannah.nongnu.org/releases/libunwind/libunwind-1.2.tar.gz"
LIBUNWIND_NAME=libunwind-1.2.tar.gz
LIBUNWIND_SOURCE=libunwind-1.2

# gperftools
GPERFTOOLS_DOWNLOAD="https://github.com/gperftools/gperftools/releases/download/gperftools-2.5.93/gperftools-2.5.93.tar.gz"
GPERFTOOLS_NAME=gperftools-2.5.93.tar.gz
GPERFTOOLS_SOURCE=gperftools-2.5.93

# zlib
ZLIB_DOWNLOAD="https://sourceforge.net/projects/libpng/files/zlib/1.2.11/zlib-1.2.11.tar.gz"
ZLIB_NAME=zlib-1.2.11.tar.gz
ZLIB_SOURCE=zlib-1.2.11 

# lz4
LZ4_DOWNLOAD="https://github.com/lz4/lz4/archive/v1.7.5.tar.gz"
LZ4_NAME=lz4-1.7.5.tar.gz
LZ4_SOURCE=lz4-1.7.5

# bzip
BZIP_DOWNLOAD="http://www.bzip.org/1.0.6/bzip2-1.0.6.tar.gz"
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

# ncurses
NCURSES_DOWNLOAD="https://ftp.gnu.org/gnu/ncurses/ncurses-6.0.tar.gz"
NCURSES_NAME=ncurses-6.0.tar.gz
NCURSES_SOURCE=ncurses-6.0

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

# all thirdparties which need to be downloaded is set in array TP_ARCHIVES
export TP_ARCHIVES=(LIBEVENT OPENSSL THRIFT LLVM CLANG COMPILER_RT PROTOBUF GFLAGS GLOG GTEST RAPIDJSON SNAPPY LIBUNWIND GPERFTOOLS ZLIB LZ4 BZIP LZO2 NCURSES CURL RE2 BOOST MYSQL BOOST_FOR_MYSQL)
