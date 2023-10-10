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
OPENSSL_DOWNLOAD="https://github.com/openssl/openssl/archive/OpenSSL_1_1_1s.tar.gz"
OPENSSL_NAME=openssl-OpenSSL_1_1_1s.tar.gz
OPENSSL_SOURCE=openssl-OpenSSL_1_1_1s
OPENSSL_MD5SUM="7e79a7560dee77c0758baa33c61af4b4"

# thrift
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.16.0/thrift-0.16.0.tar.gz"
THRIFT_NAME=thrift-0.16.0.tar.gz
THRIFT_SOURCE=thrift-0.16.0
THRIFT_MD5SUM="44cf1b54b4ec1890576c85804acfa637"

# protobuf
# brpc is not yet compatible with protobuf >= 22
PROTOBUF_DOWNLOAD="https://github.com/protocolbuffers/protobuf/releases/download/v21.11/protobuf-all-21.11.tar.gz"
PROTOBUF_NAME="protobuf-all-21.11.tar.gz"
PROTOBUF_SOURCE=protobuf-21.11
PROTOBUF_MD5SUM="b3b104f0374802e1add5d5d7a5a845ac"

# gflags
GFLAGS_DOWNLOAD="https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"
GFLAGS_NAME=gflags-2.2.2.tar.gz
GFLAGS_SOURCE=gflags-2.2.2
GFLAGS_MD5SUM="1a865b93bacfa963201af3f75b7bd64c"

# glog
GLOG_DOWNLOAD="https://github.com/google/glog/archive/refs/tags/v0.6.0.tar.gz"
GLOG_NAME="glog-v0.6.0.tar.gz"
GLOG_SOURCE=glog-0.6.0
GLOG_MD5SUM="c98a6068bc9b8ad9cebaca625ca73aa2"

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
LZ4_DOWNLOAD="https://github.com/lz4/lz4/archive/v1.9.4.tar.gz"
LZ4_NAME=lz4-1.9.4.tar.gz
LZ4_SOURCE=lz4-1.9.4
LZ4_MD5SUM="e9286adb64040071c5e23498bf753261"

# bzip
BZIP_DOWNLOAD="https://fossies.org/linux/misc/bzip2-1.0.8.tar.gz"
BZIP_NAME=bzip2-1.0.8.tar.gz
BZIP_SOURCE=bzip2-1.0.8
BZIP_MD5SUM="67e051268d0c475ea773822f7500d0e5"

# lzo2
LZO2_DOWNLOAD="https://fossies.org/linux/misc/lzo-2.10.tar.gz"
LZO2_NAME=lzo-2.10.tar.gz
LZO2_SOURCE=lzo-2.10
LZO2_MD5SUM="39d3f3f9c55c87b1e5d6888e1420f4b5"

# rapidjson
RAPIDJSON_DOWNLOAD="https://github.com/Tencent/rapidjson/archive/1a803826f1197b5e30703afe4b9c0e7dd48074f5.zip"
RAPIDJSON_NAME=rapidjson-1a803826f1197b5e30703afe4b9c0e7dd48074f5.zip
RAPIDJSON_SOURCE=rapidjson-1a803826f1197b5e30703afe4b9c0e7dd48074f5
RAPIDJSON_MD5SUM="f2212a77e055a15501477f1e390007ea"

# curl
CURL_DOWNLOAD="https://curl.se/download/curl-8.2.1.tar.gz"
CURL_NAME="curl-8.2.1.tar.gz"
CURL_SOURCE=curl-8.2.1
CURL_MD5SUM="b25588a43556068be05e1624e0e74d41"

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
BOOST_DOWNLOAD="https://boostorg.jfrog.io/artifactory/main/release/1.81.0/source/boost_1_81_0.tar.gz"
BOOST_NAME=boost_1_81_0.tar.gz
BOOST_SOURCE=boost_1_81_0
BOOST_MD5SUM="4bf02e84afb56dfdccd1e6aec9911f4b"

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
BRPC_DOWNLOAD="https://github.com/apache/brpc/archive/refs/tags/1.4.0.tar.gz"
BRPC_NAME="brpc-1.4.0.tar.gz"
BRPC_SOURCE="brpc-1.4.0"
BRPC_MD5SUM="6af9d50822c33a3abc56a1ec0af0e0bc"

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
ZSTD_DOWNLOAD="https://github.com/facebook/zstd/releases/download/v1.5.5/zstd-1.5.5.tar.gz"
ZSTD_NAME=zstd-1.5.5.tar.gz
ZSTD_SOURCE=zstd-1.5.5
ZSTD_MD5SUM="63251602329a106220e0a5ad26ba656f"

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

# c-ares
CARES_DOWNLOAD="https://github.com/c-ares/c-ares/releases/download/cares-1_19_1/c-ares-1.19.1.tar.gz"
CARES_NAME="c-ares-1.19.1.tar.gz"
CARES_SOURCE=c-ares-1.19.1
CARES_MD5SUM="dafc5825a92dc907e144570e4e75a908"

# grpc
# grpc v1.55 and above require protobuf >= 22
GRPC_DOWNLOAD="https://github.com/grpc/grpc/archive/refs/tags/v1.54.3.tar.gz"
GRPC_NAME="grpc-v1.54.3.tar.gz"
GRPC_SOURCE=grpc-1.54.3
GRPC_MD5SUM="af00a2edeae0f02bb25917cc3473b7de"

# arrow
ARROW_DOWNLOAD="https://github.com/apache/arrow/archive/refs/tags/apache-arrow-13.0.0.tar.gz"
ARROW_NAME="apache-arrow-13.0.0.tar.gz"
ARROW_SOURCE="arrow-apache-arrow-13.0.0"
ARROW_MD5SUM="8ec1ec6a119514bcaea1cf7aabc9df1f"

# Abseil
ABSEIL_DOWNLOAD="https://github.com/abseil/abseil-cpp/archive/refs/tags/20230125.3.tar.gz"
ABSEIL_NAME="abseil-cpp-20230125.3.tar.gz"
ABSEIL_SOURCE=abseil-cpp-20230125.3
ABSEIL_MD5SUM="9b6dae642c4bd92f007ab2c148bc0498"

# S2
S2_DOWNLOAD="https://github.com/google/s2geometry/archive/refs/tags/v0.10.0.tar.gz"
S2_NAME=s2geometry-0.10.0.tar.gz
S2_SOURCE=s2geometry-0.10.0
S2_MD5SUM="c68f3c5d326dde9255681b9201393a9f"

# bitshuffle
BITSHUFFLE_DOWNLOAD="https://github.com/kiyo-masui/bitshuffle/archive/0.5.1.tar.gz"
BITSHUFFLE_NAME=bitshuffle-0.5.1.tar.gz
BITSHUFFLE_SOURCE=bitshuffle-0.5.1
BITSHUFFLE_MD5SUM="b3bf6a9838927f7eb62214981c138e2f"

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
PARALLEL_HASHMAP_DOWNLOAD="https://github.com/greg7mdp/parallel-hashmap/archive/refs/tags/v1.3.8.tar.gz"
PARALLEL_HASHMAP_NAME="parallel-hashmap-1.3.8.tar.gz"
PARALLEL_HASHMAP_SOURCE="parallel-hashmap-1.3.8"
PARALLEL_HASHMAP_MD5SUM="1b8130d0b4f656257ef654699bfbf941"

# orc
ORC_DOWNLOAD="https://archive.apache.org/dist/orc/orc-1.9.0/orc-1.9.0.tar.gz"
ORC_NAME="orc-1.9.0.tar.gz"
ORC_SOURCE=orc-1.9.0
ORC_MD5SUM="5dc1c91c4867e4519aab531ffc30fab7"

# jemalloc for arrow
JEMALLOC_ARROW_DOWNLOAD="https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2"
JEMALLOC_ARROW_NAME="jemalloc-5.3.0.tar.bz2"
JEMALLOC_ARROW_SOURCE="jemalloc-5.3.0"
JEMALLOC_ARROW_MD5SUM="09a8328574dab22a7df848eae6dbbf53"

# jemalloc for doris
JEMALLOC_DORIS_DOWNLOAD="https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2"
JEMALLOC_DORIS_NAME="jemalloc-5.3.0.tar.bz2"
JEMALLOC_DORIS_SOURCE="jemalloc-5.3.0"
JEMALLOC_DORIS_MD5SUM="09a8328574dab22a7df848eae6dbbf53"

# libunwind
LIBUNWIND_DOWNLOAD="http://download.savannah.nongnu.org/releases/libunwind/libunwind-1.6.2.tar.gz"
LIBUNWIND_NAME="libunwind-1.6.2.tar.gz"
LIBUNWIND_SOURCE="libunwind-1.6.2"
LIBUNWIND_MD5SUM="f625b6a98ac1976116c71708a73dc44a"

# cctz
CCTZ_DOWNLOAD="https://github.com/google/cctz/archive/v2.3.tar.gz"
CCTZ_NAME="cctz-2.3.tar.gz"
CCTZ_SOURCE="cctz-2.3"
CCTZ_MD5SUM="209348e50b24dbbdec6d961059c2fc92"

# datatables, bootstrap 3 and jQuery 3
# The origin download url is always changing: https://datatables.net/download/builder?bs-3.3.7/jq-3.3.1/dt-1.10.25
# So we put it in our own http server.
# If someone can offer an official url for DataTables, please update this.
DATATABLES_DOWNLOAD="https://github.com/apache/doris-thirdparty/releases/download/datatables-1.12.1/DataTables.zip"
DATATABLES_NAME="DataTables.zip"
DATATABLES_SOURCE="DataTables-1.12.1"
DATATABLES_MD5SUM="a3dd92a2a8b7254443e102a43036d743"

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
AWS_SDK_DOWNLOAD="https://github.com/aws/aws-sdk-cpp/archive/refs/tags/1.11.119.tar.gz"
AWS_SDK_NAME="aws-sdk-cpp-1.11.119.tar.gz"
AWS_SDK_SOURCE="aws-sdk-cpp-1.11.119"
AWS_SDK_MD5SUM="3cd8bd51d39dc207a243a2074d11f439"

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
IDN_DOWNLOAD="https://ftpmirror.gnu.org/libidn/libidn-1.38.tar.gz"
IDN_NAME="libidn-1.38.tar.gz"
IDN_SOURCE="libidn-1.38"
IDN_MD5SUM="718ff3700dd71f830c592ebe97249193"

# gsasl
GSASL_DOWNLOAD="https://ftpmirror.gnu.org/gsasl/libgsasl-1.8.0.tar.gz"
GSASL_NAME="libgsasl-1.8.0.tar.gz"
GSASL_SOURCE="libgsasl-1.8.0"
GSASL_MD5SUM="5dbdf859f6e60e05813370e2b193b92b"

# krb5
KRB5_DOWNLOAD="https://kerberos.org/dist/krb5/1.19/krb5-1.19.tar.gz"
KRB5_NAME="krb5-1.19.tar.gz"
KRB5_SOURCE="krb5-1.19"
KRB5_MD5SUM="aaf18447a5a014aa3b7e81814923f4c9"

# hdfs3
HDFS3_DOWNLOAD="https://github.com/apache/doris-thirdparty/archive/refs/tags/libhdfs3-v2.3.9.tar.gz"
HDFS3_NAME="doris-thirdparty-libhdfs3-v2.3.9.tar.gz"
HDFS3_SOURCE="doris-thirdparty-libhdfs3-v2.3.9"
HDFS3_MD5SUM="b3eaa03e5b184521e5ad5bf6cabea97e"

#libdivide
LIBDIVIDE_DOWNLOAD="https://github.com/ridiculousfish/libdivide/archive/5.0.tar.gz"
LIBDIVIDE_NAME="libdivide-5.0.tar.gz"
LIBDIVIDE_SOURCE="libdivide-5.0"
LIBDIVIDE_MD5SUM="7fd16b0bb4ab6812b2e2fdc7bfb81641"

#pdqsort
PDQSORT_DOWNLOAD="https://raw.githubusercontent.com/orlp/pdqsort/b1ef26a55cdb60d236a5cb199c4234c704f46726/pdqsort.h"
PDQSORT_NAME="pdqsort.h"
PDQSORT_FILE="pdqsort.h"
PDQSORT_MD5SUM="af28f79d5d7d7a5486f54d9f1244c2b5"

# benchmark
BENCHMARK_DOWNLOAD="https://github.com/google/benchmark/archive/refs/tags/v1.8.0.tar.gz"
BENCHMARK_NAME=benchmark-v1.8.0.tar.gz
BENCHMARK_SOURCE=benchmark-1.8.0
BENCHMARK_MD5SUM="8ddf8571d3f6198d37852bcbd964f817"

# xsimd
# for arrow-13.0.0, if arrow upgrade, this version may also need to be changed
XSIMD_DOWNLOAD="https://github.com/xtensor-stack/xsimd/archive/refs/tags/9.0.1.tar.gz"
XSIMD_NAME="xsimd-9.0.1.tar.gz"
XSIMD_SOURCE=xsimd-9.0.1
XSIMD_MD5SUM="59f38fe3364acd7ed137771258812d6c"

# simdjson
SIMDJSON_DOWNLOAD="https://github.com/simdjson/simdjson/archive/refs/tags/v3.0.1.tar.gz"
SIMDJSON_NAME=simdjson-3.0.1.tar.gz
SIMDJSON_SOURCE=simdjson-3.0.1
SIMDJSON_MD5SUM="993576b47249f2bade2bfb2552b2896a"

# nlohmann_json
NLOHMANN_JSON_DOWNLOAD="https://github.com/nlohmann/json/archive/refs/tags/v3.10.1.tar.gz"
NLOHMANN_JSON_NAME=json-3.10.1.tar.gz
NLOHMANN_JSON_SOURCE=json-3.10.1
NLOHMANN_JSON_MD5SUM="7b369d567afc0dffdcf5800fd9abb836"

# opentelemetry-proto
OPENTELEMETRY_PROTO_DOWNLOAD="https://github.com/open-telemetry/opentelemetry-proto/archive/refs/tags/v1.0.0.tar.gz"
OPENTELEMETRY_PROTO_NAME="opentelemetry-proto-v1.0.0.tar.gz"
OPENTELEMETRY_PROTO_SOURCE=opentelemetry-proto-1.0.0
OPENTELEMETRY_PROTO_MD5SUM="8c7495a0dceea7cfdbdbcd53b07436dc"

# opentelemetry
OPENTELEMETRY_DOWNLOAD="https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/v1.10.0.tar.gz"
OPENTELEMETRY_NAME="opentelemetry-cpp-v1.10.0.tar.gz"
OPENTELEMETRY_SOURCE=opentelemetry-cpp-1.10.0
OPENTELEMETRY_MD5SUM="89169762241b2f5142b728c775173283"

# libbacktrace
LIBBACKTRACE_DOWNLOAD="https://codeload.github.com/ianlancetaylor/libbacktrace/zip/2446c66076480ce07a6bd868badcbceb3eeecc2e"
LIBBACKTRACE_NAME=libbacktrace-2446c66076480ce07a6bd868badcbceb3eeecc2e.zip
LIBBACKTRACE_SOURCE=libbacktrace-2446c66076480ce07a6bd868badcbceb3eeecc2e
LIBBACKTRACE_MD5SUM="6c79a8012870a24610c0d9c3621b23fe"

# sse2noen
SSE2NEON_DOWNLOAD="https://github.com/DLTcollab/sse2neon/archive/refs/tags/v1.6.0.tar.gz"
SSE2NEON_NAME=sse2neon-1.6.0.tar.gz
SSE2NEON_SOURCE=sse2neon-1.6.0
SSE2NEON_MD5SUM="dce28eb6a78f45bf98740d5fad73febb"

# xxhash
XXHASH_DOWNLOAD="https://github.com/Cyan4973/xxHash/archive/refs/tags/v0.8.1.tar.gz"
XXHASH_NAME=xxHash-0.8.1.tar.gz
XXHASH_SOURCE=xxHash-0.8.1
XXHASH_MD5SUM="b67c587f5ff4894253da0095ba7ea393"

# concurrentqueue
CONCURRENTQUEUE_DOWNLOAD="https://github.com/cameron314/concurrentqueue/archive/refs/tags/v1.0.3.tar.gz"
CONCURRENTQUEUE_NAME=concurrentqueue-1.0.3.tar.gz
CONCURRENTQUEUE_SOURCE=concurrentqueue-1.0.3
CONCURRENTQUEUE_MD5SUM="118e5bb661b567634647312991e10222"

# fast_float
FAST_FLOAT_DOWNLOAD="https://github.com/fastfloat/fast_float/archive/refs/tags/v3.9.0.tar.gz"
FAST_FLOAT_NAME=fast_float-3.9.0.tar.gz
FAST_FLOAT_SOURCE=fast_float-3.9.0
FAST_FLOAT_MD5SUM="5656b0d8b150a3b157cfb092d214f6ea"

# libhdfs
HADOOP_LIBS_DOWNLOAD="https://github.com/apache/doris-thirdparty/archive/refs/tags/hadoop-3.3.4.5-for-doris.tar.gz"
HADOOP_LIBS_NAME="hadoop-3.3.4.5-for-doris.tar.gz"
HADOOP_LIBS_SOURCE="doris-thirdparty-hadoop-3.3.4.5-for-doris"
HADOOP_LIBS_MD5SUM="15b7be1747b27c37923b0cb9db6cff8c"

# libdragonbox for faster double/float to string
DRAGONBOX_DOWNLOAD="https://github.com/jk-jeon/dragonbox/archive/refs/tags/1.1.3.tar.gz"
DRAGONBOX_NAME=dragonbox-1.1.3.tar.gz
DRAGONBOX_SOURCE=dragonbox-1.1.3
DRAGONBOX_MD5SUM="889dc00db9612c6949a4ccf8115e0e6a"

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
    'ABSEIL'
    'S2'
    'BITSHUFFLE'
    'CROARINGBITMAP'
    'FMT'
    'PARALLEL_HASHMAP'
    'ORC'
    'CARES'
    'GRPC'
    'JEMALLOC_ARROW'
    'JEMALLOC_DORIS'
    'LIBUNWIND'
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
    'CONCURRENTQUEUE'
    'FAST_FLOAT'
    'HADOOP_LIBS'
    'DRAGONBOX'
)

if [[ "$(uname -s)" == 'Darwin' ]]; then
    #binutils
    BINUTILS_DOWNLOAD='https://ftpmirror.gnu.org/gnu/binutils/binutils-2.39.tar.gz'
    BINUTILS_NAME=binutils-2.39.tar.gz
    BINUTILS_SOURCE=binutils-2.39
    BINUTILS_MD5SUM='ab6825df57514ec172331e988f55fc10'

    #gettext
    GETTEXT_DOWNLOAD='https://ftpmirror.gnu.org/gettext/gettext-0.21.tar.gz'
    GETTEXT_NAME='gettext-0.21.tar.gz'
    GETTEXT_SOURCE='gettext-0.21'
    GETTEXT_MD5SUM='28b1cd4c94a74428723ed966c38cf479'

    read -r -a TP_ARCHIVES <<<"${TP_ARCHIVES[*]} BINUTILS GETTEXT"
    export TP_ARCHIVES
fi
