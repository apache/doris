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

cmake_minimum_required(VERSION 3.19.2)

project(doris CXX C)

# Write compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# set platforms

if (CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
    set (ARCH_AMD64 1)
endif ()
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*)")
    set (ARCH_AARCH64 1)
endif ()
if (ARCH_AARCH64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "arm")
    set (ARCH_ARM 1)
endif ()
if (CMAKE_LIBRARY_ARCHITECTURE MATCHES "i386")
    set (ARCH_I386 1)
endif ()
if ((ARCH_ARM AND NOT ARCH_AARCH64) OR ARCH_I386)
    message (FATAL_ERROR "32bit platforms are not supported")
endif ()

if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(ppc64le.*|PPC64LE.*)")
    set (ARCH_PPC64LE 1)
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
endif ()

option(GLIBC_COMPATIBILITY "Enable compatibility with older glibc libraries." ON)
message(STATUS "GLIBC_COMPATIBILITY is ${GLIBC_COMPATIBILITY}")

# set CMAKE_BUILD_TYPE
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
endif()

string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)
message(STATUS "Build type is ${CMAKE_BUILD_TYPE}")

# set CMAKE_BUILD_TARGET_ARCH
# use `lscpu | grep 'Architecture' | awk '{print $2}'` only support system which language is en_US.UTF-8
execute_process(COMMAND bash "-c" "uname -m"
                OUTPUT_VARIABLE
                CMAKE_BUILD_TARGET_ARCH
                OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Build target arch is ${CMAKE_BUILD_TARGET_ARCH}")

# Set dirs
set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(ENV{DORIS_HOME} "${BASE_DIR}/../")
set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(THIRDPARTY_DIR "$ENV{DORIS_THIRDPARTY}/installed/")
set(GENSRC_DIR "${BASE_DIR}/../gensrc/build/")
set(SRC_DIR "${BASE_DIR}/src/")
set(TEST_DIR "${CMAKE_SOURCE_DIR}/test/")
set(OUTPUT_DIR "${BASE_DIR}/output")

if (APPLE)
    set(MAKE_TEST "ON")
else()
    option(MAKE_TEST "ON for make unit test or OFF for not" OFF)
endif()
message(STATUS "make test: ${MAKE_TEST}")
option(WITH_MYSQL "Support access MySQL" ON)

# Check gcc
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11.0")
        message(FATAL_ERROR "Need GCC version at least 11.0")
    endif()
endif()

set(CMAKE_SKIP_RPATH TRUE)
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_STATIC_RUNTIME ON)

# Compile generated source if necessary
message(STATUS "build gensrc if necessary")
execute_process(COMMAND make -C ${BASE_DIR}/../gensrc/
                RESULT_VARIABLE MAKE_GENSRC_RESULT)
if(NOT ${MAKE_GENSRC_RESULT} EQUAL 0 AND NOT APPLE)
    message(FATAL_ERROR "Failed to build ${BASE_DIR}/../gensrc/")
endif()

# Set Boost
set(Boost_DEBUG FALSE)
set(Boost_USE_MULTITHREADED ON)
set(BOOST_ROOT ${THIRDPARTY_DIR})
set(Boost_NO_BOOST_CMAKE OFF)

if (NOT APPLE)
    find_package(Boost 1.73.0 REQUIRED COMPONENTS system date_time)
else()
    find_package(Boost 1.73.0 COMPONENTS system date_time)
endif()

set(GPERFTOOLS_HOME "${THIRDPARTY_DIR}/gperftools")

# Set all libraries
add_library(gflags STATIC IMPORTED)
set_target_properties(gflags PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgflags.a)

add_library(glog STATIC IMPORTED)
set_target_properties(glog PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libglog.a)

add_library(backtrace STATIC IMPORTED)
set_target_properties(backtrace PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbacktrace.a)

add_library(re2 STATIC IMPORTED)
set_target_properties(re2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libre2.a)

add_library(odbc STATIC IMPORTED)
set_target_properties(odbc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libodbc.a)

add_library(pprof STATIC IMPORTED)
set_target_properties(pprof PROPERTIES IMPORTED_LOCATION
    ${GPERFTOOLS_HOME}/lib/libprofiler.a)

add_library(tcmalloc STATIC IMPORTED)
set_target_properties(tcmalloc PROPERTIES IMPORTED_LOCATION
    ${GPERFTOOLS_HOME}/lib/libtcmalloc.a)

add_library(protobuf STATIC IMPORTED)
set_target_properties(protobuf PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotobuf.a)

add_library(protoc STATIC IMPORTED)
set_target_properties(protoc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotoc.a)

add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgtest.a)

add_library(gtest_main STATIC IMPORTED)
set_target_properties(gtest_main PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgtest_main.a)

add_library(benchmark STATIC IMPORTED)
set_target_properties(benchmark PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbenchmark.a)

add_library(gmock STATIC IMPORTED)
set_target_properties(gmock PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgmock.a)

add_library(snappy STATIC IMPORTED)
set_target_properties(snappy PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsnappy.a)

add_library(curl STATIC IMPORTED)
set_target_properties(curl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcurl.a)

add_library(lz4 STATIC IMPORTED)
set_target_properties(lz4 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/liblz4.a)

add_library(thrift STATIC IMPORTED)
set_target_properties(thrift PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libthrift.a)

add_library(thriftnb STATIC IMPORTED)
set_target_properties(thriftnb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libthriftnb.a)

if(WITH_LZO)
    add_library(lzo STATIC IMPORTED)
    set_target_properties(lzo PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/liblzo2.a)
endif()

if (WITH_MYSQL)
    add_library(mysql STATIC IMPORTED)
    set_target_properties(mysql PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libmysqlclient.a)
endif()

add_library(libevent STATIC IMPORTED)
set_target_properties(libevent PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libevent.a)

add_library(libevent_pthreads STATIC IMPORTED)
set_target_properties(libevent_pthreads PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libevent_pthreads.a)

add_library(libbz2 STATIC IMPORTED)
set_target_properties(libbz2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbz2.a)

add_library(libz STATIC IMPORTED)
set_target_properties(libz PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libz.a)

add_library(crypto STATIC IMPORTED)
set_target_properties(crypto PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcrypto.a)

add_library(openssl STATIC IMPORTED)
set_target_properties(openssl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libssl.a)

add_library(leveldb STATIC IMPORTED)
set_target_properties(leveldb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libleveldb.a)

add_library(jemalloc STATIC IMPORTED)
set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libjemalloc.a)

add_library(brotlicommon STATIC IMPORTED)
set_target_properties(brotlicommon PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrotlicommon.a)

add_library(brotlidec STATIC IMPORTED)
set_target_properties(brotlidec PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrotlidec.a)

add_library(brotlienc STATIC IMPORTED)
set_target_properties(brotlienc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrotlienc.a)

add_library(zstd STATIC IMPORTED)
set_target_properties(zstd PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libzstd.a)

add_library(arrow STATIC IMPORTED)
set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libarrow.a)

add_library(parquet STATIC IMPORTED)
set_target_properties(parquet PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libparquet.a)

add_library(brpc STATIC IMPORTED)
set_target_properties(brpc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrpc.a)

add_library(rocksdb STATIC IMPORTED)
set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librocksdb.a)

add_library(cyrus-sasl STATIC IMPORTED)
set_target_properties(cyrus-sasl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsasl2.a)

add_library(librdkafka_cpp STATIC IMPORTED)
set_target_properties(librdkafka_cpp PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librdkafka++.a)

add_library(librdkafka STATIC IMPORTED)
set_target_properties(librdkafka PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librdkafka.a)

add_library(libs2 STATIC IMPORTED)
set_target_properties(libs2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libs2.a)

add_library(bitshuffle STATIC IMPORTED)
set_target_properties(bitshuffle PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbitshuffle.a)

add_library(roaring STATIC IMPORTED)
set_target_properties(roaring PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libroaring.a)

add_library(fmt STATIC IMPORTED)
set_target_properties(fmt PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libfmt.a)

add_library(orc STATIC IMPORTED)
set_target_properties(orc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/liborc.a)

add_library(cctz STATIC IMPORTED)
set_target_properties(cctz PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcctz.a)

add_library(aws-sdk-core STATIC IMPORTED)
set_target_properties(aws-sdk-core PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-cpp-sdk-core.a)

add_library(aws-sdk-s3 STATIC IMPORTED)
set_target_properties(aws-sdk-s3 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-cpp-sdk-s3.a)

add_library(aws-sdk-transfer STATIC IMPORTED)
set_target_properties(aws-sdk-transfer PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-cpp-sdk-transfer.a)

add_library(aws-sdk-s3-crt STATIC IMPORTED)
set_target_properties(aws-sdk-s3-crt PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-cpp-sdk-s3-crt.a)


add_library(aws-crt-cpp STATIC IMPORTED)
set_target_properties(aws-crt-cpp PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-crt-cpp.a)

add_library(aws-c-cal STATIC IMPORTED)
set_target_properties(aws-c-cal PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-cal.a)

add_library(aws-c-auth STATIC IMPORTED)
set_target_properties(aws-c-auth PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-auth.a)

add_library(aws-c-compression STATIC IMPORTED)
set_target_properties(aws-c-compression PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-compression.a)

add_library(aws-c-common STATIC IMPORTED)
set_target_properties(aws-c-common PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-common.a)

add_library(aws-c-event-stream STATIC IMPORTED)
set_target_properties(aws-c-event-stream PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-event-stream.a)

add_library(aws-c-io STATIC IMPORTED)
set_target_properties(aws-c-io PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-io.a)

add_library(aws-c-http STATIC IMPORTED)
set_target_properties(aws-c-http PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-http.a)

add_library(aws-c-mqtt STATIC IMPORTED)
set_target_properties(aws-c-mqtt PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-mqtt.a)

add_library(aws-checksums STATIC IMPORTED)
set_target_properties(aws-checksums PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-checksums.a)

add_library(aws-c-s3 STATIC IMPORTED)
set_target_properties(aws-c-s3 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libaws-c-s3.a)

add_library(aws-s2n STATIC IMPORTED)
set_target_properties(aws-s2n PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libs2n.a)

add_library(minizip STATIC IMPORTED)
set_target_properties(minizip PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libminizip.a)

add_library(idn STATIC IMPORTED)
set_target_properties(idn PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libidn.a)

add_library(gsasl STATIC IMPORTED)
set_target_properties(gsasl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libgsasl.a)

add_library(breakpad STATIC IMPORTED)
set_target_properties(breakpad PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbreakpad_client.a)

if (ARCH_AMD64)
    # libhdfs3 only support x86 or amd64
    add_library(hdfs3 STATIC IMPORTED)
    set_target_properties(hdfs3 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libhdfs3.a)

    add_library(xml2 STATIC IMPORTED)
    set_target_properties(xml2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libxml2.a)

    add_library(lzma STATIC IMPORTED)
    set_target_properties(lzma PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/liblzma.a)
endif()

find_program(THRIFT_COMPILER thrift ${CMAKE_SOURCE_DIR}/bin)

# Check if functions are supported in this platform. All flags will generated
# in gensrc/build/common/env_config.h.
# You can check funcion here which depends on platform. Don't forget add this
# to be/src/common/env_config.h.in
include(CheckFunctionExists)
check_function_exists(sched_getcpu HAVE_SCHED_GETCPU)

# compiler flags that are common across debug/release builds
#  -Wall: Enable all warnings.
#  -Wno-sign-compare: suppress warnings for comparison between signed and unsigned
#    integers
#  -pthread: enable multithreaded malloc
#  -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG: enable nanosecond precision for boost
#  -fno-omit-frame-pointers: Keep frame pointer for functions in register
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall -Wno-sign-compare -pthread -Werror")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fstrict-aliasing -fno-omit-frame-pointer")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -std=gnu++17 -D__STDC_FORMAT_MACROS")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_SYSTEM_NO_DEPRECATED")
# Enable the cpu and heap profile of brpc
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBRPC_ENABLE_CPU_PROFILER")

if (USE_LLD)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fuse-ld=lld")
endif ()

if (USE_LIBCPP AND COMPILER_CLANG)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libc++ -lstdc++")
    add_definitions(-DUSE_LIBCPP)
endif()

if (COMPILER_GCC)
    # Avoid GCC 11 false alarm
    # https://stackoverflow.com/questions/67584073/gcc-11-false-array-subscript-is-partly-outside-array-bounds-warning
    # https://stackoverflow.com/questions/69426070/gcc-11-order-of-arguments-triggers-false-positive-wstringop-overflow-is-this-bu
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-array-bounds -Wno-stringop-overread")
    add_compile_options(-Wno-stringop-overflow -fdiagnostics-color=always)
endif ()

if (COMPILER_CLANG)
    add_compile_options (-fcolor-diagnostics)
    if(MAKE_TEST STREQUAL "OFF")
        add_compile_options(-Qunused-arguments)
    endif()
endif ()

# https://github.com/boostorg/uuid/issues/92
# We need to avoid using SYS_getrandom system calls
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_UUID_RANDOM_PROVIDER_FORCE_POSIX=1")
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -msse4.2")
    if (USE_AVX2)
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -mavx2")
    endif()
endif()
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS}  -Wno-attributes -DS2_USE_GFLAGS -DS2_USE_GLOG")

if (WITH_MYSQL)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DDORIS_WITH_MYSQL")
endif()

if (WITH_LZO)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DDORIS_WITH_LZO")
endif()

if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 7.0)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -faligned-new")
endif()

# For any gcc builds:
#   -g: Enable symbols for profiler tools. Produce debugging information in the operating system’s native formt
#   -Wno-unused-local-typedefs: Do not warn for local typedefs that are unused.
set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -g -Wno-unused-local-typedefs")

# For CMAKE_BUILD_TYPE=Debug
set(CXX_FLAGS_DEBUG "${CXX_GCC_FLAGS} -O0")

# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
set(CXX_FLAGS_RELEASE "${CXX_GCC_FLAGS} -O3 -DNDEBUG")

SET(CXX_FLAGS_ASAN "${CXX_GCC_FLAGS} -O0 -fsanitize=address -DADDRESS_SANITIZER")
SET(CXX_FLAGS_LSAN "${CXX_GCC_FLAGS} -O0 -fsanitize=leak -DLEAK_SANITIZER")

# Set the flags to the undefined behavior sanitizer, also known as "ubsan"
# Turn on sanitizer and debug symbols to get stack traces:
SET(CXX_FLAGS_UBSAN "${CXX_GCC_FLAGS} -O0 -fno-wrapv -fsanitize=undefined")

# Set the flags to the thread sanitizer, also known as "tsan"
# Turn on sanitizer and debug symbols to get stack traces:
# Use -Wno-builtin-declaration-mismatch to mute warnings like "new declaration ‘__tsan_atomic16 __tsan_atomic16_fetch_nand(..."
SET(CXX_FLAGS_TSAN "${CXX_GCC_FLAGS} -O0 -fsanitize=thread -DTHREAD_SANITIZER -Wno-builtin-declaration-mismatch")

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_ASAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_LSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_UBSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_TSAN}")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# Add flags that are common across build types
SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")

message(STATUS "Compiler Flags: ${CMAKE_CXX_FLAGS}")

# Thrift requires these two definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H)

# Set include dirs
include_directories(
    ${SRC_DIR}/
    ${TEST_DIR}/
)

include_directories(
    SYSTEM
    ${GENSRC_DIR}/
    ${THIRDPARTY_DIR}/include
    ${GPERFTOOLS_HOME}/include
    ${THIRDPARTY_DIR}/include/breakpad/
)

if (BUILD_JAVA_UDF)
    execute_process(COMMAND sh ${BASE_DIR}/../tools/find_libjvm.sh OUTPUT_VARIABLE LIBJVM_PATH OUTPUT_STRIP_TRAILING_WHITESPACE)
    FILE(GLOB_RECURSE LIB_JVM ${LIBJVM_PATH})
    if(${LIB_JVM} STREQUAL "")
        message(STATUS "Disable JAVA UDF because there is no libjvm found!")
    else()
        set(DORIS_DEPENDENCIES
            ${DORIS_DEPENDENCIES}
            jvm
        )
        add_library(jvm SHARED IMPORTED)
        set_target_properties(jvm PROPERTIES IMPORTED_LOCATION ${LIB_JVM})
        include_directories($ENV{JAVA_HOME}/include)
        include_directories($ENV{JAVA_HOME}/include/linux)
        add_definitions("-DLIBJVM")
    endif()
endif()

set(WL_START_GROUP "-Wl,--start-group")
set(WL_END_GROUP "-Wl,--end-group")

set(AWS_LIBS
    aws-sdk-s3
    aws-sdk-core
    aws-checksums
    aws-c-io
    aws-c-event-stream
    aws-c-common
    aws-c-cal
    aws-s2n
    aws-c-s3
    aws-c-auth
    aws-crt-cpp
    aws-c-compression
    aws-c-http
    aws-c-mqtt
    aws-sdk-s3-crt)

# Set Doris libraries
set(DORIS_LINK_LIBS
    ${WL_START_GROUP}
    Agent
    Common
    Env
    Exec
    Exprs
    Gutil
    Olap
    Rowset
    OlapFs
    Runtime
    Service
    Udf
    Util
    DorisGen
    Webserver
    Geo
    Vec
    ${WL_END_GROUP}
)
if (${MAKE_TEST} STREQUAL "ON")
    set(DORIS_LINK_LIBS
        ${DORIS_LINK_LIBS}
        TestUtil
    )
endif()


# COMMON_THIRDPARTY are thirdparty dependencies that can run on all platform
# When adding new dependencies, If you don’t know if it can run on all platforms,
# add it here first.
set(COMMON_THIRDPARTY
    backtrace
    rocksdb
    cyrus-sasl
    libs2
    snappy
    Boost::date_time
    thrift
    thriftnb
    glog
    re2
    pprof
    lz4
    libevent
    libevent_pthreads
    idn
    gsasl
    curl
    libz
    libbz2
    gflags
    brpc
    protobuf
    openssl
    crypto
    leveldb
    bitshuffle
    roaring
    fmt
    jemalloc
    brotlicommon
    brotlidec
    brotlienc
    zstd
    arrow
    parquet
    orc
    odbc
    cctz
    minizip
    breakpad
    ${AWS_LIBS}
    # put this after lz4 to avoid using lz4 lib in librdkafka
    librdkafka_cpp
    librdkafka
)

if (${MAKE_TEST} STREQUAL "ON")
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        benchmark
    )
endif()

# thirdparties dependescies that can only run on X86 platform
set(X86_DEPENDENCIES
    ${COMMON_THIRDPARTY}
    hdfs3
    xml2
    lzma
)

if(ARCH_AARCH64)
    # Set thirdparty libraries
    set(DORIS_DEPENDENCIES
        ${DORIS_DEPENDENCIES}
        ${WL_START_GROUP}
        ${COMMON_THIRDPARTY}
    )
else()
    set(DORIS_DEPENDENCIES
        ${DORIS_DEPENDENCIES}
        ${WL_START_GROUP}
        ${X86_DEPENDENCIES}
    )
endif()

if(WITH_LZO)
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES}
        lzo
    )
endif()

if (WITH_MYSQL)
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES}
        mysql
        )
endif()

set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} ${WL_END_GROUP})

message(STATUS "DORIS_DEPENDENCIES is ${DORIS_DEPENDENCIES}")

# Add all external dependencies. They should come after the palo libs.
# static link gcc's lib
set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
    ${DORIS_DEPENDENCIES}
    -static-libstdc++
    -static-libgcc
    -lstdc++fs
)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(ASAN_LIBS -static-libasan)
    set(LSAN_LIBS -static-liblsan)
    set(UBSAN_LIBS -static-libubsan tcmalloc)
    set(TSAN_LIBS -static-libtsan)
else ()
    set(UBSAN_LIBS -rtlib=compiler-rt tcmalloc)
endif ()

# Add sanitize static link flags or tcmalloc
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} tcmalloc)
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${ASAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${LSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${UBSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${TSAN_LIBS})
    add_definitions("-DTHREAD_SANITIZER")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# NOTE(amos): This should come before -lc -lm to interpose symbols correctly.
if (GLIBC_COMPATIBILITY)
    add_subdirectory(${SRC_DIR}/glibc-compatibility)
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} glibc-compatibility-explicit glibc-compatibility)
endif()

set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
    -lrt -l:libbfd.a -liberty -lc -lm -ldl -pthread
)

# Set libraries for test
set (TEST_LINK_LIBS ${DORIS_LINK_LIBS}
    ${WL_START_GROUP}
    Test_util
    gmock
    gtest
    gtest_main
    ${WL_END_GROUP}
)

# Only build static libs
set(BUILD_SHARED_LIBS OFF)

if (${MAKE_TEST} STREQUAL "ON")
    if (USE_LLD)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fuse-ld=lld")
    endif ()

    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage -DGTEST_USE_OWN_TR1_TUPLE=0")
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -lgcov")
    add_definitions(-DBE_TEST)
    add_subdirectory(${SRC_DIR}/testutil)
endif ()

add_subdirectory(${SRC_DIR}/agent)
add_subdirectory(${SRC_DIR}/common)
add_subdirectory(${SRC_DIR}/env)
add_subdirectory(${SRC_DIR}/exec)
add_subdirectory(${SRC_DIR}/exprs)
add_subdirectory(${SRC_DIR}/gen_cpp)
add_subdirectory(${SRC_DIR}/geo)
add_subdirectory(${SRC_DIR}/gutil)
add_subdirectory(${SRC_DIR}/http)
add_subdirectory(${SRC_DIR}/olap)
add_subdirectory(${SRC_DIR}/runtime)
add_subdirectory(${SRC_DIR}/service)
add_subdirectory(${SRC_DIR}/udf)

if (BUILD_META_TOOL AND BUILD_META_TOOL STREQUAL "ON")
    add_subdirectory(${SRC_DIR}/tools)
endif()

add_subdirectory(${SRC_DIR}/util)
add_subdirectory(${SRC_DIR}/vec)

# Utility CMake function to make specifying tests and benchmarks less verbose
FUNCTION(ADD_BE_TEST TEST_NAME)
    # use argn to add additional files
    set(ADDITIONAL_FILES ${ARGN})
    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/")
    # This gets the directory where the test is from (e.g. 'exprs' or 'runtime')
    get_filename_component(DIR_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)
    get_filename_component(TEST_DIR_NAME ${TEST_NAME} PATH)
    get_filename_component(TEST_FILE_NAME ${TEST_NAME} NAME)

    ADD_EXECUTABLE(${TEST_FILE_NAME} ${TEST_NAME}.cpp ${ADDITIONAL_FILES})
    TARGET_LINK_LIBRARIES(${TEST_FILE_NAME} ${TEST_LINK_LIBS})
    SET_TARGET_PROPERTIES(${TEST_FILE_NAME} PROPERTIES COMPILE_FLAGS "-fno-access-control" ENABLE_EXPORTS 1)
    if (NOT "${TEST_DIR_NAME}" STREQUAL "")
        SET_TARGET_PROPERTIES(${TEST_FILE_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}/${TEST_DIR_NAME}")
    endif()
    ADD_TEST(${TEST_FILE_NAME} "${BUILD_OUTPUT_ROOT_DIRECTORY}/${TEST_NAME}")
ENDFUNCTION()

if (${MAKE_TEST} STREQUAL "ON")
    add_subdirectory(${TEST_DIR}/test_util)
    add_subdirectory(${TEST_DIR}/agent)
    add_subdirectory(${TEST_DIR}/common)
    add_subdirectory(${TEST_DIR}/env)
    add_subdirectory(${TEST_DIR}/exec)
    add_subdirectory(${TEST_DIR}/exprs)
    add_subdirectory(${TEST_DIR}/geo)
    add_subdirectory(${TEST_DIR}/gutil)
    add_subdirectory(${TEST_DIR}/http)
    add_subdirectory(${TEST_DIR}/olap)
    add_subdirectory(${TEST_DIR}/runtime)
    add_subdirectory(${TEST_DIR}/udf)
    add_subdirectory(${TEST_DIR}/util)
    add_subdirectory(${TEST_DIR}/vec/core)
    add_subdirectory(${TEST_DIR}/vec/exec)
    add_subdirectory(${TEST_DIR}/vec/exprs)
    add_subdirectory(${TEST_DIR}/vec/function)
    add_subdirectory(${TEST_DIR}/vec/runtime)
    add_subdirectory(${TEST_DIR}/vec/aggregate_functions)
    add_subdirectory(${TEST_DIR}/tools)
endif ()

# Install be
install(DIRECTORY DESTINATION ${OUTPUT_DIR})
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/bin)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/conf)

install(FILES
    ${BASE_DIR}/../bin/start_be.sh
    ${BASE_DIR}/../bin/stop_be.sh
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${OUTPUT_DIR}/bin)

install(FILES
    ${BASE_DIR}/../conf/be.conf
    ${BASE_DIR}/../conf/odbcinst.ini
    DESTINATION ${OUTPUT_DIR}/conf)

get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach(dir ${dirs})
    message(STATUS "dir='${dir}'")
endforeach()
