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

# ============================================================================
# Doris Thirdparty CMake Build
# ============================================================================
#
# This CMakeLists.txt builds thirdparty libraries from source under src/.
# Source code is downloaded & patched by download-thirdparty.sh (unchanged).
#
# Usage from BE:
#   cmake -DUSE_CONTRIB_SOURCE=ON ...
#   This will add_subdirectory(thirdparty/) to build deps from source,
#   inheriting BE's compiler flags (ASAN, TSAN, Debug, Release, etc.)
#
# ============================================================================

# Guard: this CMakeLists builds thirdparty from source. BE only adds it via
# add_subdirectory when USE_CONTRIB_SOURCE=ON. If somehow included with
# USE_CONTRIB_SOURCE=OFF, bail out — be/cmake/thirdparty.cmake's IMPORTED
# targets are the active ones in that mode.
if (DEFINED USE_CONTRIB_SOURCE AND NOT USE_CONTRIB_SOURCE)
    message(FATAL_ERROR
        "thirdparty/CMakeLists.txt entered with USE_CONTRIB_SOURCE=OFF — this "
        "shouldn't happen; BE should have included be/cmake/thirdparty.cmake instead.")
endif()

set(TP_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(TP_PATCH_DIR "${CMAKE_CURRENT_SOURCE_DIR}/patches")
set(TP_INSTALL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/installed")

# Fail-fast: thirdparty/src/ is .gitignore'd, fresh clones have an empty dir.
# build.sh auto-runs download-thirdparty.sh, but if cmake is invoked directly,
# this guard tells the user how to populate the tree.
file(GLOB _tp_src_entries RELATIVE "${TP_SOURCE_DIR}" "${TP_SOURCE_DIR}/*")
list(LENGTH _tp_src_entries _tp_src_count)
if (_tp_src_count LESS 10)
    message(FATAL_ERROR
        "thirdparty/src/ has only ${_tp_src_count} entries — source-mode build "
        "needs the thirdparty source tree downloaded first. Run:\n"
        "  bash thirdparty/download-thirdparty.sh\n"
        "Or use build.sh which does this automatically:\n"
        "  ./build.sh --be")
endif()

# Add shim Find modules to CMAKE_MODULE_PATH so that find_package() calls
# from grpc/thrift/rocksdb find our add_subdirectory-built targets
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/shims")

# Skip all install rules for thirdparty libs — we build them, not install them
# This fixes INSTALL(EXPORT) errors for gflags/thrift/rocksdb/aws-sdk
set(CMAKE_SKIP_INSTALL_RULES ON)
# Workaround: Parent CMakeLists still tries to include our missing install script
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/cmake_install.cmake" "# Dummy script to satisfy parent include()\n")

# Suppress all warnings in third-party code
set(TP_SAVED_C_FLAGS "${CMAKE_C_FLAGS}")
set(TP_SAVED_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Common settings for all third-party libs
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)


# Proto/gRPC code generation utilities (used by brpc, grpc, arrow)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/proto_utils.cmake)

# ============================================================================
# Helper: include a cmake wrapper if exists
# ============================================================================
function(add_contrib name)
    set(cmake_file "${CMAKE_CURRENT_SOURCE_DIR}/cmake/${name}.cmake")
    if(EXISTS "${cmake_file}")
        message(STATUS "[contrib] Adding ${name}")
        include(${cmake_file})
    else()
        message(WARNING "[contrib] No cmake wrapper for ${name}, skipping")
    endif()
endfunction()



# ============================================================================
# Layer 0: Header-only libraries (no compilation needed)
# ============================================================================
add_contrib(rapidjson)
add_contrib(parallel_hashmap)
add_contrib(fast_float)
add_contrib(libdivide)
add_contrib(pdqsort)
add_contrib(timsort)
add_contrib(concurrentqueue)
add_contrib(nlohmann_json)

# ============================================================================
# Layer 1: Simple libraries with no/minimal dependencies
# ============================================================================
add_contrib(xxhash)
add_contrib(zlib)
add_contrib(minizip)          # depends: zlib (in zlib/contrib/minizip)
add_contrib(lz4)
add_contrib(zstd)
add_contrib(snappy)
add_contrib(brotli)
add_contrib(bzip)
add_contrib(lzo2)
add_contrib(crc32c)
add_contrib(fmt)
add_contrib(simdjson)
add_contrib(streamvbyte)
add_contrib(base64)
add_contrib(libdeflate)
add_contrib(libbacktrace)
add_contrib(roaring)
add_contrib(bitshuffle)
add_contrib(dragonbox)
add_contrib(cctz)
add_contrib(pugixml)

# ============================================================================
# Layer 2: Libraries with moderate dependencies
# ============================================================================
add_contrib(openssl)         # autoconf (Configure + make)
add_contrib(gflags)
add_contrib(glog)            # depends: gflags
add_contrib(gtest)
add_contrib(benchmark)
add_contrib(abseil)
add_contrib(protobuf)        # depends: zlib, abseil
add_contrib(flatbuffers)
add_contrib(re2)
add_contrib(leveldb)         # depends: snappy
add_contrib(boost)           # autoconf (b2)
add_contrib(s2)              # depends: abseil, openssl
add_contrib(icu)             # autoconf (runConfigureICU)
add_contrib(libunwind)       # autoconf
add_contrib(jemalloc)        # autoconf
add_contrib(gperftools)      # autoconf

# ============================================================================
# Layer 3: Libraries with heavy dependencies
# ============================================================================
add_contrib(libevent)
add_contrib(curl)            # depends: openssl
add_contrib(cares)
add_contrib(grpc)            # depends: protobuf, abseil, c-ares, re2, zlib
add_contrib(thrift)          # depends: openssl, libevent, boost
add_contrib(rocksdb)         # depends: zlib, lz4, zstd, snappy
add_contrib(brpc)            # depends: protobuf, gflags, glog, leveldb
# TODO: fix hyperscan and arrow integration
add_contrib(ragel)           # build tool needed by hyperscan
add_contrib(hyperscan)       # depends: boost, ragel
add_contrib(arrow)           # depends: many
add_contrib(rdkafka)         # depends: zstd, lz4, cyrus-sasl

# ============================================================================
# Layer 4: System/auth libraries (autoconf)
# ============================================================================
add_contrib(krb5)            # autoconf
add_contrib(krb5_headers)    # copy krb5 headers from old/include
add_contrib(cyrus_sasl)      # autoconf
add_contrib(gsasl)           # autoconf
add_contrib(idn)             # autoconf
add_contrib(xml2)            # autoconf
add_contrib(lzma)            # autoconf
add_contrib(odbc)            # autoconf
add_contrib(mysql)           # headers only
add_contrib(libunwind_headers)  # copy libunwind headers from old/include
add_contrib(hadoop_hdfs_headers) # copy hadoop_hdfs_3_4 headers from old/include

# ============================================================================
# Layer 5: Cloud/Storage SDKs
# ============================================================================
add_contrib(aws_sdk)         # depends: curl, openssl, zlib
add_contrib(azure)           # depends: curl, openssl
add_contrib(ali_sdk)

# ============================================================================
# Layer 6: Other
# ============================================================================
add_contrib(hadoop_hdfs)     # JNI
add_contrib(jsoncpp)
add_contrib(libuuid)         # autoconf
add_contrib(paimon_cpp)
add_contrib(xsimd)

# Restore compiler flags
set(CMAKE_C_FLAGS "${TP_SAVED_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${TP_SAVED_CXX_FLAGS}")

# ============================================================================
# COMMON_THIRDPARTY: library list for doris_be target_link_libraries
# This replaces cmake/thirdparty.cmake — uses actual target names from source build.
# ============================================================================
set(COMMON_THIRDPARTY
    # Core libs
    rapidjson
    parallel_hashmap
    fast_float
    libdivide_header
    pdqsort
    timsort
    aws_c_headers
    concurrentqueue
    nlohmann_json
    xxhash
    dragonbox
    lzo2
    gflags
    glog
    backtrace
    re2
    odbc
    libprotobuf
    gtest
    gtest_main
    benchmark
    gmock
    snappy
    snappy_headers
    curl
    lz4
    lz4_headers
    thrift
    thriftnb
    crc32c
    jemalloc
    mysql_headers
    boost_headers
    boost_container

    # libevent
    libevent_core
    libevent_openssl
    libevent
    libevent_pthreads

    # compression/encoding
    libbz2
    libz
    brotlicommon
    brotlidec
    brotlienc
    zstd

    # openssl
    crypto
    openssl

    # leveldb
    leveldb

    # grpc
    grpc++_reflection
    grpc
    grpc++
    grpc++_unsecure
    gpr
    upb
    cares
    address_sorting
    z

    # arrow — Arrow creates _static suffix targets for static builds
    arrow_static
    arrow_flight_static
    arrow_flight_sql_static
    arrow_dataset_static
    arrow_acero_static
    parquet_static

    # brpc
    brpc

    # rocksdb
    rocksdb

    # auth libs
    cyrus-sasl
    rdkafka_cpp
    rdkafka
    rdkafka_headers

    # misc
    s2
    bitshuffle
    roaring
    fmt
    cctz
    base64
    minizip

    # aws sdk
    aws-cpp-sdk-core
    aws-cpp-sdk-s3
    aws-cpp-sdk-transfer
    aws-cpp-sdk-s3-crt
    aws-crt-cpp
    aws-c-cal
    aws-c-auth
    aws-c-compression
    aws-c-common
    aws-c-event-stream
    aws-c-io
    aws-c-http
    aws-c-mqtt
    aws-checksums
    aws-c-s3
    aws-c-sdkutils
    aws-cpp-sdk-identity-management
    aws-cpp-sdk-sts
    aws-cpp-sdk-kinesis
    s2n

    # simdjson
    simdjson

    # auth
    idn
    xml2
    lzma
    gsasl
    krb5support
    krb5
    com_err
    k5crypto
    gssapi_krb5

    # other
    streamvbyte
    deflate
    hs
    icui18n
    icuuc
    icudata
    pugixml
    PARENT_SCOPE
)

# ============================================================================
# Fix INTERFACE_INCLUDE_DIRECTORIES for targets built via add_subdirectory.
# When BE links COMMON_THIRDPARTY, these include dirs propagate automatically.
# This replaces the old global thirdparty/installed/include approach.
# ============================================================================

# zstd — include dirs now set in zstd.cmake
# lz4 — include dirs now set in lz4.cmake
# glog — include dirs now set in glog.cmake
# fmt — include dirs now set in fmt.cmake

# protobuf — include dirs now set in protobuf.cmake

# thrift — include dirs now set in thrift.cmake

# brpc/bvar — include dirs set directly in cmake/brpc.cmake

# arrow — include dirs now set in arrow.cmake

# AWS SDK — include dirs now set in aws_sdk.cmake

# cyrus-sasl — sasl/sasl.h include is set in cyrus_sasl.cmake
# (cannot use target_include_directories on ALIAS targets)

# streamvbyte — include dirs now set in streamvbyte.cmake

# rdkafka — needs zstd, lz4, sasl include dirs for its own compilation
if(TARGET rdkafka)
    target_include_directories(rdkafka PRIVATE
        ${TP_SOURCE_DIR}/zstd-1.5.7/lib
        ${TP_SOURCE_DIR}/lz4-1.9.4/lib
        ${TP_SOURCE_DIR}/cyrus-sasl-2.1.27/include)
endif()

# gperftools/tcmalloc target
if(TARGET pprof)
    set(PPROF_TARGET pprof)
else()
    set(PPROF_TARGET "")
endif()
set(GPERFTOOLS_LIBS ${PPROF_TARGET} PARENT_SCOPE)

# Azure SDK targets (if built)
set(AZURE_LIBS "")
if(TARGET azure-core)
    set(AZURE_LIBS azure-core azure-identity azure-storage-blobs azure-storage-common)
endif()
set(AZURE_THIRDPARTY ${AZURE_LIBS} PARENT_SCOPE)

# Paimon targets (if built)
set(PAIMON_LIBS "")
if(ENABLE_PAIMON_CPP)
    set(PAIMON_LIBS
        paimon_parquet_file_format
        paimon_orc_file_format
        paimon_blob_file_format
        paimon_local_file_system
        paimon_file_index
        paimon_global_index
        roaring_bitmap_paimon
        xxhash_paimon
        fmt_paimon
        tbb_paimon
        paimon
    )
endif()
set(PAIMON_THIRDPARTY ${PAIMON_LIBS} PARENT_SCOPE)
