#!/bin/bash
# shellcheck disable=2034

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

###################################################
# DO NOT change variables bellow unless you known
# what you are doing.
###################################################

# thirdparties will be downloaded and unpacked here
export TP_SOURCE_DIR="${TP_DIR:-.}/src"

# thirdparties will be installed to here
export TP_INSTALL_DIR="${TP_DIR:-.}/installed"

# patches for all thirdparties
export TP_PATCH_DIR="${TP_DIR:-.}/patches"

# header files of all thirdparties will be intalled to here
export TP_INCLUDE_DIR="${TP_INSTALL_DIR}/include"

# libraries of all thirdparties will be intalled to here
export TP_LIB_DIR="${TP_INSTALL_DIR}/lib"

# all java libraries will be unpacked to here
export TP_JAR_DIR="${TP_INSTALL_DIR}/lib/jar"

# source of all dependencies, default unuse it
# export REPOSITORY_URL=

#####################################################
# Download url, filename and unpaced filename
# of all thirdparties
#####################################################

# libevent
LIBEVENT_DOWNLOAD="https://github.com/libevent/libevent/archive/release-2.1.12-stable.tar.gz"
LIBEVENT_NAME=libevent-release-2.1.12-stable.tar.gz
LIBEVENT_SOURCE=libevent-release-2.1.12-stable
LIBEVENT_MD5SUM="0d5a27436bf7ff8253420c8cf09f47ca"

# openssl
OPENSSL_DOWNLOAD="https://github.com/openssl/openssl/archive/OpenSSL_1_1_1m.tar.gz"
OPENSSL_NAME=openssl-OpenSSL_1_1_1m.tar.gz
OPENSSL_SOURCE=openssl-OpenSSL_1_1_1m
OPENSSL_MD5SUM="710c2368d28f1a25ab92e25b5b9b11ec"

# thrift
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz"
THRIFT_NAME=thrift-0.13.0.tar.gz
THRIFT_SOURCE=thrift-0.13.0
THRIFT_MD5SUM="38a27d391a2b03214b444cb13d5664f1"

# protobuf
PROTOBUF_DOWNLOAD="https://github.com/google/protobuf/archive/v3.14.0.tar.gz"
PROTOBUF_NAME=protobuf-3.14.0.tar.gz
PROTOBUF_SOURCE=protobuf-3.14.0
PROTOBUF_MD5SUM="0c9d2a96f3656ba7ef3b23b533fb6170"

# gflags
GFLAGS_DOWNLOAD="https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"
GFLAGS_NAME=gflags-2.2.2.tar.gz
GFLAGS_SOURCE=gflags-2.2.2
GFLAGS_MD5SUM="1a865b93bacfa963201af3f75b7bd64c"

# glog
GLOG_DOWNLOAD="https://github.com/google/glog/archive/v0.4.0.tar.gz"
GLOG_NAME=glog-0.4.0.tar.gz
GLOG_SOURCE=glog-0.4.0
GLOG_MD5SUM="0daea8785e6df922d7887755c3d100d0"

# gtest
GTEST_DOWNLOAD="https://github.com/google/googletest/archive/release-1.11.0.tar.gz"
GTEST_NAME=googletest-release-1.11.0.tar.gz
GTEST_SOURCE=googletest-release-1.11.0
GTEST_MD5SUM="e8a8df240b6938bb6384155d4c37d937"

# snappy
SNAPPY_DOWNLOAD="https://github.com/google/snappy/archive/1.1.8.tar.gz"
SNAPPY_NAME=snappy-1.1.8.tar.gz
SNAPPY_SOURCE=snappy-1.1.8
SNAPPY_MD5SUM="70e48cba7fecf289153d009791c9977f"

# gperftools
GPERFTOOLS_DOWNLOAD="https://github.com/gperftools/gperftools/releases/download/gperftools-2.10/gperftools-2.10.tar.gz"
GPERFTOOLS_NAME=gperftools-2.10.tar.gz
GPERFTOOLS_SOURCE=gperftools-2.10
GPERFTOOLS_MD5SUM="62bf6c76ba855ed580de5e139bd2a483"

# zlib
ZLIB_DOWNLOAD="https://sourceforge.net/projects/libpng/files/zlib/1.2.11/zlib-1.2.11.tar.gz"
ZLIB_NAME=zlib-1.2.11.tar.gz
ZLIB_SOURCE=zlib-1.2.11
ZLIB_MD5SUM="1c9f62f0778697a09d36121ead88e08e"

# lz4
LZ4_DOWNLOAD="https://github.com/lz4/lz4/archive/v1.9.3.tar.gz"
LZ4_NAME=lz4-1.9.3.tar.gz
LZ4_SOURCE=lz4-1.9.3
LZ4_MD5SUM="3a1ab1684e14fc1afc66228ce61b2db3"

# bzip
BZIP_DOWNLOAD="https://fossies.org/linux/misc/bzip2-1.0.8.tar.gz"
BZIP_DOWNLOAD="ftp://sourceware.org/pub/bzip2/bzip2-1.0.8.tar.gz"
BZIP_NAME=bzip2-1.0.8.tar.gz
BZIP_SOURCE=bzip2-1.0.8
BZIP_MD5SUM="67e051268d0c475ea773822f7500d0e5"

# lzo2
LZO2_DOWNLOAD="http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz"
LZO2_NAME=lzo-2.10.tar.gz
LZO2_SOURCE=lzo-2.10
LZO2_MD5SUM="39d3f3f9c55c87b1e5d6888e1420f4b5"

# rapidjson
RAPIDJSON_DOWNLOAD="https://github.com/Tencent/rapidjson/archive/1a803826f1197b5e30703afe4b9c0e7dd48074f5.zip"
RAPIDJSON_NAME=rapidjson-1a803826f1197b5e30703afe4b9c0e7dd48074f5.zip
RAPIDJSON_SOURCE=rapidjson-1a803826f1197b5e30703afe4b9c0e7dd48074f5
RAPIDJSON_MD5SUM="f2212a77e055a15501477f1e390007ea"

# curl
CURL_DOWNLOAD="https://curl.se/download/curl-7.79.0.tar.gz"
CURL_NAME=curl-7.79.0.tar.gz
CURL_SOURCE=curl-7.79.0
CURL_MD5SUM="b40e4dc4bbc9e109c330556cd58c8ec8"

# RE2
RE2_DOWNLOAD="https://github.com/google/re2/archive/2021-02-02.tar.gz"
RE2_NAME=re2-2021-02-02.tar.gz
RE2_SOURCE=re2-2021-02-02
RE2_MD5SUM="48bc665463a86f68243c5af1bac75cd0"

# hyperscan
HYPERSCAN_DOWNLOAD="https://github.com/intel/hyperscan/archive/refs/tags/v5.4.0.tar.gz"
HYPERSCAN_NAME=hyperscan-5.4.0.tar.gz
HYPERSCAN_SOURCE=hyperscan-5.4.0
HYPERSCAN_MD5SUM="65e08385038c24470a248f6ff2fa379b"

# vectorscan (support arm for hyperscan)
MACHINE_TYPE=$(uname -m)
if [[ "${MACHINE_TYPE}" == "aarch64" || "${MACHINE_TYPE}" == 'arm64' ]]; then
    echo "use vectorscan instead of hyperscan on aarch64"
    HYPERSCAN_DOWNLOAD="https://github.com/VectorCamp/vectorscan/archive/refs/tags/vectorscan/5.4.7.tar.gz"
    HYPERSCAN_NAME=vectorscan-5.4.7.tar.gz
    HYPERSCAN_SOURCE=vectorscan-vectorscan-5.4.7
    HYPERSCAN_MD5SUM="ae924ccce79ef9bf6bf118693ae14fe5"
fi

# ragel (dependency for hyperscan)
RAGEL_DOWNLOAD="http://www.colm.net/files/ragel/ragel-6.10.tar.gz"
RAGEL_NAME=ragel-6.10.tar.gz
RAGEL_SOURCE=ragel-6.10
RAGEL_MD5SUM="748cae8b50cffe9efcaa5acebc6abf0d"

# boost
BOOST_DOWNLOAD="https://boostorg.jfrog.io/artifactory/main/release/1.73.0/source/boost_1_73_0.tar.gz"
BOOST_NAME=boost_1_73_0.tar.gz
BOOST_SOURCE=boost_1_73_0
BOOST_MD5SUM="4036cd27ef7548b8d29c30ea10956196"

# mysql
MYSQL_DOWNLOAD="https://github.com/mysql/mysql-server/archive/mysql-5.7.18.tar.gz"
MYSQL_NAME=mysql-5.7.18.tar.gz
MYSQL_SOURCE=mysql-server-mysql-5.7.18
MYSQL_MD5SUM="58598b10dce180e4d1fbdd7cf5fa68d6"

# unix odbc
ODBC_DOWNLOAD="http://www.unixodbc.org/unixODBC-2.3.7.tar.gz"
ODBC_NAME=unixODBC-2.3.7.tar.gz
ODBC_SOURCE=unixODBC-2.3.7
ODBC_MD5SUM="274a711b0c77394e052db6493840c6f9"

# leveldb
LEVELDB_DOWNLOAD="https://github.com/google/leveldb/archive/refs/tags/1.23.tar.gz"
LEVELDB_NAME=leveldb-1.23.tar.gz
LEVELDB_SOURCE=leveldb-1.23
LEVELDB_MD5SUM="afbde776fb8760312009963f09a586c7"

# brpc
BRPC_DOWNLOAD="https://github.com/apache/incubator-brpc/archive/refs/tags/1.2.0.tar.gz"
BRPC_NAME="incubator-brpc-1.2.0.tar.gz"
BRPC_SOURCE="incubator-brpc-1.2.0"
BRPC_MD5SUM="556c024d5f770dbd2336ca4541ae8c96"

# rocksdb
ROCKSDB_DOWNLOAD="https://github.com/facebook/rocksdb/archive/v5.14.2.tar.gz"
ROCKSDB_NAME=rocksdb-5.14.2.tar.gz
ROCKSDB_SOURCE=rocksdb-5.14.2
ROCKSDB_MD5SUM="b72720ea3b1e9ca9e4ed0febfef65b14"

# cyrus-sasl
CYRUS_SASL_DOWNLOAD="https://github.com/cyrusimap/cyrus-sasl/releases/download/cyrus-sasl-2.1.27/cyrus-sasl-2.1.27.tar.gz"
CYRUS_SASL_NAME=cyrus-sasl-2.1.27.tar.gz
CYRUS_SASL_SOURCE=cyrus-sasl-2.1.27
CYRUS_SASL_MD5SUM="a33820c66e0622222c5aefafa1581083"

# librdkafka-1.8.2
LIBRDKAFKA_DOWNLOAD="https://github.com/edenhill/librdkafka/archive/refs/tags/v1.8.2.tar.gz"
LIBRDKAFKA_NAME=librdkafka-1.8.2.tar.gz
LIBRDKAFKA_SOURCE=librdkafka-1.8.2
LIBRDKAFKA_MD5SUM="0abec0888d10c9553cdcbcbf9172d558"

# zstd
ZSTD_DOWNLOAD="https://github.com/facebook/zstd/releases/download/v1.5.2/zstd-1.5.2.tar.gz"
ZSTD_NAME=zstd-1.5.2.tar.gz
ZSTD_SOURCE=zstd-1.5.2
ZSTD_MD5SUM="072b10f71f5820c24761a65f31f43e73"

# brotli
BROTLI_DOWNLOAD="https://github.com/google/brotli/archive/v1.0.9.tar.gz"
BROTLI_NAME="brotli-1.0.9.tar.gz"
BROTLI_SOURCE="brotli-1.0.9"
BROTLI_MD5SUM="c2274f0c7af8470ad514637c35bcee7d"

# flatbuffers
FLATBUFFERS_DOWNLOAD="https://github.com/google/flatbuffers/archive/v2.0.0.tar.gz"
FLATBUFFERS_NAME=flatbuffers-2.0.0.tar.gz
FLATBUFFERS_SOURCE=flatbuffers-2.0.0
FLATBUFFERS_MD5SUM="a27992324c3cbf86dd888268a23d17bd"

# arrow
ARROW_DOWNLOAD="https://dlcdn.apache.org/arrow/arrow-7.0.0/apache-arrow-7.0.0.tar.gz"
ARROW_NAME="apache-arrow-7.0.0.tar.gz"
ARROW_SOURCE="apache-arrow-7.0.0"
ARROW_MD5SUM="316ade159901646849b3b4760fa52816"

# S2
S2_DOWNLOAD="https://github.com/google/s2geometry/archive/v0.9.0.tar.gz"
S2_NAME=s2geometry-0.9.0.tar.gz
S2_SOURCE=s2geometry-0.9.0
S2_MD5SUM="293552c7646193b8b4a01556808fe155"

# bitshuffle
BITSHUFFLE_DOWNLOAD="https://github.com/kiyo-masui/bitshuffle/archive/0.3.5.tar.gz"
BITSHUFFLE_NAME=bitshuffle-0.3.5.tar.gz
BITSHUFFLE_SOURCE=bitshuffle-0.3.5
BITSHUFFLE_MD5SUM="2648ec7ccd0b896595c6636d926fc867"

# croaringbitmap
CROARINGBITMAP_DOWNLOAD="https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v0.4.0.tar.gz"
CROARINGBITMAP_NAME=CRoaring-0.4.0.tar.gz
CROARINGBITMAP_SOURCE=CRoaring-0.4.0
CROARINGBITMAP_MD5SUM="7c5cb6f2089cedc5ad9373f538a83334"

# fmt
FMT_DOWNLOAD="https://github.com/fmtlib/fmt/archive/7.1.3.tar.gz"
FMT_NAME="fmt-7.1.3.tar.gz"
FMT_SOURCE="fmt-7.1.3"
FMT_MD5SUM="2522ec65070c0bda0ca288677ded2831"

# parallel-hashmap
PARALLEL_HASHMAP_DOWNLOAD="https://github.com/greg7mdp/parallel-hashmap/archive/1.33.tar.gz"
PARALLEL_HASHMAP_NAME="parallel-hashmap-1.33.tar.gz"
PARALLEL_HASHMAP_SOURCE="parallel-hashmap-1.33"
PARALLEL_HASHMAP_MD5SUM="7626b5215f745c4ce59b5a4e41d16235"

# orc
ORC_DOWNLOAD="https://archive.apache.org/dist/orc/orc-1.7.2/orc-1.7.2.tar.gz"
ORC_NAME=orc-1.7.2.tar.gz
ORC_SOURCE=orc-1.7.2
ORC_MD5SUM="6cab37935eacdec7d078d327746a8578"

# jemalloc
JEMALLOC_DOWNLOAD="https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2"
JEMALLOC_NAME="jemalloc-5.2.1.tar.bz2"
JEMALLOC_SOURCE="jemalloc-5.2.1"
JEMALLOC_MD5SUM="3d41fbf006e6ebffd489bdb304d009ae"

# cctz
CCTZ_DOWNLOAD="https://github.com/google/cctz/archive/v2.3.tar.gz"
CCTZ_NAME="cctz-2.3.tar.gz"
CCTZ_SOURCE="cctz-2.3"
CCTZ_MD5SUM="209348e50b24dbbdec6d961059c2fc92"

# datatables, bootstrap 3 and jQuery 3
# The origin download url is always changing: https://datatables.net/download/builder?bs-3.3.7/jq-3.3.1/dt-1.10.25
# So we put it in our own http server.
# If someone can offer an official url for DataTables, please update this.
DATATABLES_DOWNLOAD="https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/DataTables.zip"
DATATABLES_NAME="DataTables.zip"
DATATABLES_SOURCE="DataTables-1.10.25"
DATATABLES_MD5SUM="c8fd73997c9871e213ee4211847deed5"

# bootstrap table js
BOOTSTRAP_TABLE_JS_DOWNLOAD="https://unpkg.com/bootstrap-table@1.17.1/dist/bootstrap-table.min.js"
BOOTSTRAP_TABLE_JS_NAME="bootstrap-table.min.js"
BOOTSTRAP_TABLE_JS_FILE="bootstrap-table.min.js"
BOOTSTRAP_TABLE_JS_MD5SUM="6cc9c41eaf7e81e54e220061cc9c0432"

# bootstrap table css
BOOTSTRAP_TABLE_CSS_DOWNLOAD="https://unpkg.com/bootstrap-table@1.17.1/dist/bootstrap-table.min.css"
BOOTSTRAP_TABLE_CSS_NAME="bootstrap-table.min.css"
BOOTSTRAP_TABLE_CSS_FILE="bootstrap-table.min.css"
BOOTSTRAP_TABLE_CSS_MD5SUM="23389d4456da412e36bae30c469a766a"

# aws sdk
AWS_SDK_DOWNLOAD="https://github.com/aws/aws-sdk-cpp/archive/refs/tags/1.9.211.tar.gz"
AWS_SDK_NAME="aws-sdk-cpp-1.9.211.tar.gz"
AWS_SDK_SOURCE="aws-sdk-cpp-1.9.211"
AWS_SDK_MD5SUM="667b8e08baf0b9967c19224198e33160"

# tsan_header
TSAN_HEADER_DOWNLOAD="https://gcc.gnu.org/git/?p=gcc.git;a=blob_plain;f=libsanitizer/include/sanitizer/tsan_interface_atomic.h;hb=refs/heads/releases/gcc-7"
TSAN_HEADER_NAME="tsan_interface_atomic.h"
TSAN_HEADER_FILE="tsan_interface_atomic.h"
TSAN_HEADER_MD5SUM="d72679bea167d6a513d959f5abd149dc"

# lzma
LZMA_DOWNLOAD="https://github.com/kobolabs/liblzma/archive/refs/heads/master.zip"
LZMA_NAME="liblzma-master.zip"
LZMA_SOURCE="liblzma-master"
LZMA_MD5SUM="ef11f2fbbfa6893b629f207a32bf730e"

# xml2
XML2_DOWNLOAD="https://gitlab.gnome.org/GNOME/libxml2/-/archive/v2.9.10/libxml2-v2.9.10.tar.gz"
XML2_NAME="libxml2-v2.9.10.tar.gz"
XML2_SOURCE="libxml2-v2.9.10"
XML2_MD5SUM="b18faee9173c3378c910f6d7d1493115"

# idn
IDN_DOWNLOAD="https://ftp.gnu.org/gnu/libidn/libidn-1.38.tar.gz"
IDN_NAME="libidn-1.38.tar.gz"
IDN_SOURCE="libidn-1.38"
IDN_MD5SUM="718ff3700dd71f830c592ebe97249193"

# gsasl
GSASL_DOWNLOAD="https://ftp.gnu.org/gnu/gsasl/libgsasl-1.8.0.tar.gz"
GSASL_NAME="libgsasl-1.8.0.tar.gz"
GSASL_SOURCE="libgsasl-1.8.0"
GSASL_MD5SUM="5dbdf859f6e60e05813370e2b193b92b"

# krb5
KRB5_DOWNLOAD="https://kerberos.org/dist/krb5/1.19/krb5-1.19.tar.gz"
KRB5_NAME="krb5-1.19.tar.gz"
KRB5_SOURCE="krb5-1.19"
KRB5_MD5SUM="aaf18447a5a014aa3b7e81814923f4c9"

# hdfs3
HDFS3_DOWNLOAD="https://github.com/apache/doris-thirdparty/archive/refs/tags/libhdfs3-v2.3.2.tar.gz"
HDFS3_NAME="doris-thirdparty-libhdfs3-v2.3.2.tar.gz"
HDFS3_SOURCE="doris-thirdparty-libhdfs3-v2.3.2"
HDFS3_MD5SUM="5087ffec0fda4fbcd60a53ed92eb4d2d"

#libdivide
LIBDIVIDE_DOWNLOAD="https://github.com/ridiculousfish/libdivide/archive/5.0.tar.gz"
LIBDIVIDE_NAME="libdivide-5.0.tar.gz"
LIBDIVIDE_SOURCE="libdivide-5.0"
LIBDIVIDE_MD5SUM="7fd16b0bb4ab6812b2e2fdc7bfb81641"

#pdqsort
PDQSORT_DOWNLOAD="http://ftp.cise.ufl.edu/ubuntu/pool/universe/p/pdqsort/pdqsort_0.0.0+git20180419.orig.tar.gz"
PDQSORT_NAME="pdqsort.tar.gz"
PDQSORT_SOURCE="pdqsort-0.0.0+git20180419"
PDQSORT_MD5SUM="39261c3e7b40aa7505662fac29f22d20"

# benchmark
BENCHMARK_DOWNLOAD="https://github.com/google/benchmark/archive/v1.5.6.tar.gz"
BENCHMARK_NAME=benchmark-1.5.6.tar.gz
BENCHMARK_SOURCE=benchmark-1.5.6
BENCHMARK_MD5SUM="668b9e10d8b0795e5d461894db18db3c"

# xsimd
# for arrow-7.0.0, if arrow upgrade, this version may also need to be changed
XSIMD_DOWNLOAD="https://github.com/xtensor-stack/xsimd/archive/aeec9c872c8b475dedd7781336710f2dd2666cb2.tar.gz"
XSIMD_NAME=xsimd-aeec9c872c8b475dedd7781336710f2dd2666cb2.tar.gz
XSIMD_SOURCE=xsimd-aeec9c872c8b475dedd7781336710f2dd2666cb2
XSIMD_MD5SUM="d024855f71c0a2837a6918c0f8f66245"

# simdjson
SIMDJSON_DOWNLOAD="https://github.com/simdjson/simdjson/archive/refs/tags/v1.0.2.tar.gz"
SIMDJSON_NAME=simdjson-1.0.2.tar.gz
SIMDJSON_SOURCE=simdjson-1.0.2
SIMDJSON_MD5SUM="5bb34cca7087a99c450dbdfe406bdc7d"

# nlohmann_json
NLOHMANN_JSON_DOWNLOAD="https://github.com/nlohmann/json/archive/refs/tags/v3.10.1.tar.gz"
NLOHMANN_JSON_NAME=json-3.10.1.tar.gz
NLOHMANN_JSON_SOURCE=json-3.10.1
NLOHMANN_JSON_MD5SUM="7b369d567afc0dffdcf5800fd9abb836"

# opentelemetry-proto
OPENTELEMETRY_PROTO_DOWNLOAD="https://github.com/open-telemetry/opentelemetry-proto/archive/refs/tags/v0.18.0.tar.gz"
OPENTELEMETRY_PROTO_NAME=opentelemetry-proto-0.18.0.tar.gz
OPENTELEMETRY_PROTO_SOURCE=opentelemetry-proto-0.18.0
OPENTELEMETRY_PROTO_MD5SUM="5179f58bb4edbd805590bffd2cf4df85"

# opentelemetry
OPENTELEMETRY_DOWNLOAD="https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/v1.4.0.tar.gz"
OPENTELEMETRY_NAME=opentelemetry-cpp-1.4.0.tar.gz
OPENTELEMETRY_SOURCE=opentelemetry-cpp-1.4.0
OPENTELEMETRY_MD5SUM="511b670dd1abb596da53684d23742c5f"

# libbacktrace
LIBBACKTRACE_DOWNLOAD="https://codeload.github.com/ianlancetaylor/libbacktrace/zip/2446c66076480ce07a6bd868badcbceb3eeecc2e"
LIBBACKTRACE_NAME=libbacktrace-2446c66076480ce07a6bd868badcbceb3eeecc2e.zip
LIBBACKTRACE_SOURCE=libbacktrace-2446c66076480ce07a6bd868badcbceb3eeecc2e
LIBBACKTRACE_MD5SUM="6c79a8012870a24610c0d9c3621b23fe"

# sse2noen
SSE2NEON_DOWNLOAD="https://github.com/DLTcollab/sse2neon/archive/refs/tags/v1.5.1.tar.gz"
SSE2NEON_NAME=sse2neon-1.5.1.tar.gz
SSE2NEON_SOURCE=sse2neon-1.5.1
SSE2NEON_MD5SUM="9de5dc2970aa7efac7faee59e2826c51"

# xxhash
XXHASH_DOWNLOAD="https://github.com/Cyan4973/xxHash/archive/refs/tags/v0.8.1.tar.gz"
XXHASH_NAME=xxHash-0.8.1.tar.gz
XXHASH_SOURCE=xxHash-0.8.1
XXHASH_MD5SUM="b67c587f5ff4894253da0095ba7ea393"

# all thirdparties which need to be downloaded is set in array TP_ARCHIVES
export TP_ARCHIVES=(
    'LIBEVENT'
    'OPENSSL'
    'THRIFT'
    'PROTOBUF'
    'GFLAGS'
    'GLOG'
    'GTEST'
    'RAPIDJSON'
    'SNAPPY'
    'GPERFTOOLS'
    'ZLIB'
    'LZ4'
    'BZIP'
    'LZO2'
    'CURL'
    'RE2'
    'HYPERSCAN'
    'RAGEL'
    'BOOST'
    'MYSQL'
    'ODBC'
    'LEVELDB'
    'BRPC'
    'ROCKSDB'
    'CYRUS_SASL'
    'LIBRDKAFKA'
    'FLATBUFFERS'
    'ARROW'
    'BROTLI'
    'ZSTD'
    'S2'
    'BITSHUFFLE'
    'CROARINGBITMAP'
    'FMT'
    'PARALLEL_HASHMAP'
    'ORC'
    'JEMALLOC'
    'CCTZ'
    'DATATABLES'
    'BOOTSTRAP_TABLE_JS'
    'BOOTSTRAP_TABLE_CSS'
    'TSAN_HEADER'
    'AWS_SDK'
    'LZMA'
    'XML2'
    'IDN'
    'GSASL'
    'KRB5'
    'HDFS3'
    'LIBDIVIDE'
    'PDQSORT'
    'BENCHMARK'
    'XSIMD'
    'SIMDJSON'
    'NLOHMANN_JSON'
    'OPENTELEMETRY_PROTO'
    'OPENTELEMETRY'
    'LIBBACKTRACE'
    'SSE2NEON'
    'XXHASH'
)

if [[ "$(uname -s)" == 'Darwin' ]]; then
    #binutils
    BINUTILS_DOWNLOAD='https://mirrors.aliyun.com/gnu/binutils/binutils-2.39.tar.gz'
    BINUTILS_NAME=binutils-2.39.tar.gz
    BINUTILS_SOURCE=binutils-2.39
    BINUTILS_MD5SUM='ab6825df57514ec172331e988f55fc10'

    #gettext
    GETTEXT_DOWNLOAD='https://mirrors.aliyun.com/gnu/gettext/gettext-0.21.tar.gz'
    GETTEXT_NAME='gettext-0.21.tar.gz'
    GETTEXT_SOURCE='gettext-0.21'
    GETTEXT_MD5SUM='28b1cd4c94a74428723ed966c38cf479'

    read -r -a TP_ARCHIVES <<<"${TP_ARCHIVES[*]} BINUTILS GETTEXT"
    export TP_ARCHIVES
fi
