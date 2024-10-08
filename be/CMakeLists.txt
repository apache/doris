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
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|arm64.*)")
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

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set (OS_LINUX 1)
    add_definitions(-D OS_LINUX)
elseif (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set (OS_MACOSX 1)
    add_definitions(-D OS_MACOSX)
endif ()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
    option(ENABLE_PCH "enable pch" OFF)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
    option(ENABLE_PCH "enable pch" ON)
endif ()

# Set boost/stacktrace use backtrace api to unwind
if (NOT OS_MACOSX)
    add_definitions(-DBOOST_STACKTRACE_USE_BACKTRACE)
else()
    add_definitions(-DBOOST_STACKTRACE_USE_NOOP)
endif()

# enable glog custom prefix
add_definitions(-DGLOG_CUSTOM_PREFIX_SUPPORT)

# Options
option(GLIBC_COMPATIBILITY "Enable compatibility with older glibc libraries." ON)
option(USE_LIBCPP "Use libc++" OFF)
option(USE_MEM_TRACKER, "Use memory tracker" ON)
option(USE_UNWIND "Use libunwind" ON)
option(USE_JEMALLOC "Use jemalloc" ON)
if (OS_MACOSX)
    set(GLIBC_COMPATIBILITY OFF)
    set(USE_LIBCPP ON)
    set(USE_MEM_TRACKER OFF)
    set(USE_UNWIND OFF)
endif()

if (DISPLAY_BUILD_TIME)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "time -f 'TimeUsage: real=%es, user=%Us, sys=%Ss'")
endif()

message(STATUS "GLIBC_COMPATIBILITY is ${GLIBC_COMPATIBILITY}")
message(STATUS "USE_LIBCPP is ${USE_LIBCPP}")
message(STATUS "USE_MEM_TRACKER is ${USE_MEM_TRACKER}")
message(STATUS "USE_JEMALLOC is ${USE_JEMALLOC}")
message(STATUS "USE_UNWIND is ${USE_UNWIND}")
message(STATUS "ENABLE_PCH is ${ENABLE_PCH}")

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

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "aarch64" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "arm64")
    set (ARCH_ARM 1)
endif()

# Set dirs
set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(ENV{DORIS_HOME} "${BASE_DIR}/..")
set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(GENSRC_DIR "${BASE_DIR}/../gensrc/build/")
set(COMMON_SRC_DIR "${BASE_DIR}/../common")
set(SRC_DIR "${BASE_DIR}/src/")
set(TEST_DIR "${CMAKE_SOURCE_DIR}/test/")
set(OUTPUT_DIR "${BASE_DIR}/output")
if (NOT DEFINED ENV{DORIS_THIRDPARTY})
    set(ENV{DORIS_THIRDPARTY} "$ENV{DORIS_HOME}/thirdparty")
endif()
set(THIRDPARTY_DIR "$ENV{DORIS_THIRDPARTY}/installed")
message(STATUS "THIRDPARTY_DIR is ${THIRDPARTY_DIR}")

option(MAKE_TEST "ON for make unit test or OFF for not" OFF)
message(STATUS "make test: ${MAKE_TEST}")

option(WITH_MYSQL "Support access MySQL" ON)

option(BUILD_FS_BENCHMARK "ON for building fs benchmark tool or OFF for not" OFF)
message(STATUS "build fs benchmark tool: ${BUILD_FS_BENCHMARK}")

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
set(Boost_ROOT ${THIRDPARTY_DIR})
set(Boost_NO_BOOST_CMAKE OFF)
set(BOOST_VERSION "1.81.0")

if (NOT APPLE)
    find_package(Boost ${BOOST_VERSION} REQUIRED COMPONENTS system date_time)
    find_package(Boost ${BOOST_VERSION} REQUIRED COMPONENTS system container)
else()
    find_package(Boost ${BOOST_VERSION} COMPONENTS system date_time)
    find_package(Boost ${BOOST_VERSION} COMPONENTS system container)
endif()

# Set if use libazure or not
option(BUILD_AZURE "ON for building azure support for BE or OFF for not" OFF)
message(STATUS "build azure: ${BUILD_AZURE}")
if(BUILD_AZURE STREQUAL "ON")
    add_definitions(-DUSE_AZURE)
endif()


set(GPERFTOOLS_HOME "${THIRDPARTY_DIR}/gperftools")

include (cmake/thirdparty.cmake)

find_program(THRIFT_COMPILER thrift ${CMAKE_SOURCE_DIR}/bin)

set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(BUILD_JAVA OFF)
set(BUILD_CPP_TESTS OFF)
set(STOP_BUILD_ON_WARNING OFF)
set(BUILD_LIBHDFSPP OFF)
SET(PROTOBUF_HOME "$ENV{DORIS_THIRDPARTY}/installed")
SET(SNAPPY_HOME "$ENV{DORIS_THIRDPARTY}/installed")
SET(LZ4_HOME "$ENV{DORIS_THIRDPARTY}/installed")
SET(LZ4_INCLUDE_DIR "$ENV{DORIS_THIRDPARTY}/installed/include/lz4")
SET(ZLIB_HOME "$ENV{DORIS_THIRDPARTY}/installed")
SET(ZSTD_HOME "$ENV{DORIS_THIRDPARTY}/installed")
SET(ZSTD_INCLUDE_DIR "$ENV{DORIS_THIRDPARTY}/installed/include/zstd")

add_subdirectory(${SRC_DIR}/apache-orc EXCLUDE_FROM_ALL)
target_compile_options(orc PRIVATE -Wno-implicit-fallthrough -w)

option(BUILD_STATIC_LIBRARIES "Build static libraries" ON)
option(BUILD_SHARED_LIBRARIES "Build shared libraries" OFF)
option(BUILD_CONTRIBS_LIB "Build contribs lib" ON)

set(BOOST_ROOT "$ENV{DORIS_THIRDPARTY}/installed")
set(ZLIB_ROOT "$ENV{DORIS_THIRDPARTY}/installed")
set(Roaring_ROOT "$ENV{DORIS_THIRDPARTY}/installed")
set(USE_STAT64 0)

# disable clucene bthread supported.
set(USE_BTHREAD OFF)

add_subdirectory(${SRC_DIR}/clucene EXCLUDE_FROM_ALL)

set(clucene_options -w -Wall)
if (COMPILER_CLANG)
    set(clucene_options ${clucene_options} -Wno-c++11-narrowing)
else ()
    set(clucene_options ${clucene_options} -Wno-narrowing)
endif()

target_compile_options(clucene-core-static PRIVATE ${clucene_options})
target_compile_options(clucene-shared-static PRIVATE ${clucene_options})
target_compile_options(clucene-contribs-lib PRIVATE ${clucene_options})
target_compile_options(ic PRIVATE ${clucene_options})

install(DIRECTORY
    ${SRC_DIR}/clucene/src/contribs-lib/CLucene/analysis/jieba/dict
    DESTINATION ${OUTPUT_DIR})

# Check if functions are supported in this platform. All flags will generated
# in gensrc/build/common/env_config.h.
# You can check funcion here which depends on platform. Don't forget add this
# to be/src/common/env_config.h.in
include(CheckFunctionExists)
check_function_exists(sched_getcpu HAVE_SCHED_GETCPU)

function(TRY_TO_CHANGE_LINKER LINKER_COMMAND LINKER_NAME)
    if (CUSTUM_LINKER_COMMAND STREQUAL "ld")
        execute_process(COMMAND ${CMAKE_C_COMPILER} -fuse-ld=${LINKER_COMMAND} -Wl,--version ERROR_QUIET OUTPUT_VARIABLE LD_VERSION)
        if ("${LD_VERSION}" MATCHES ${LINKER_NAME})
            message(STATUS "Linker ${LINKER_NAME} is available, change linker to ${LINKER_NAME}")
            set(CUSTUM_LINKER_COMMAND "${LINKER_COMMAND}" PARENT_SCOPE)
        endif()
    endif()
endfunction()

if (NOT OS_MACOSX) # MACOSX's lld will core dump
    # In terms of performance, mold> lld> gold> ld
    set(CUSTUM_LINKER_COMMAND "ld")
    TRY_TO_CHANGE_LINKER("mold" "mold")
    TRY_TO_CHANGE_LINKER("lld" "LLD")
    TRY_TO_CHANGE_LINKER("gold" "GNU gold")
    if (NOT CUSTUM_LINKER_COMMAND STREQUAL "ld")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${CUSTUM_LINKER_COMMAND}")
    endif()
endif()


set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 17)

add_compile_options(-g
                    -Wall
                    -Wextra
                    -Werror
                    -pthread
                    -fstrict-aliasing
                    -fno-omit-frame-pointer
                    $<$<COMPILE_LANGUAGE:CXX>:-Wnon-virtual-dtor>)

add_compile_options(-Wno-unused-parameter
                    -Wno-sign-compare)

if (COMPILER_GCC)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11.1")
        message(FATAL_ERROR "Need GCC version at least 11.1")
    endif()

    add_compile_options(-fdiagnostics-color=always
                        -Wno-nonnull
                        -Wno-stringop-overread
                        -Wno-stringop-overflow
                        -Wno-array-bounds)
endif ()

if (COMPILER_CLANG)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "16")
        message(FATAL_ERROR "Need Clang version at least 16")
    endif()

    add_compile_options(-fcolor-diagnostics
                        -Wpedantic
                        -Wshadow-field
                        -Wunused
                        -Wunused-command-line-argument
                        -Wunused-exception-parameter
                        -Wunused-volatile-lvalue
                        -Wunused-template
                        -Wunused-member-function
                        -Wunused-macros
                        -Wconversion)
    add_compile_options( -Wno-gnu-statement-expression
                        -Wno-implicit-float-conversion
                        -Wno-implicit-int-conversion
                        -Wno-sign-conversion
                        -Wno-missing-field-initializers
                        -Wno-unused-const-variable
                        -Wno-shorten-64-to-32)
    if (USE_LIBCPP)
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-stdlib=libc++>)
        add_definitions(-DUSE_LIBCPP)
    endif()
endif ()

add_definitions(-D__STDC_FORMAT_MACROS
                    -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG
                    -DBOOST_SYSTEM_NO_DEPRECATED
                    -DBOOST_UUID_RANDOM_PROVIDER_FORCE_POSIX=1
                    -DBRPC_ENABLE_CPU_PROFILER
                    -DS2_USE_GFLAGS
                    -DS2_USE_GLOG)

# Thrift requires these two definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H)

if (RECORD_COMPILER_SWITCHES)
    add_compile_options(-frecord-gcc-switches)
endif()

# simd for architectures
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    add_compile_options(-msse4.2)
    if (USE_AVX2)
        add_compile_options(-mavx2)
    endif()
endif()

if (ARCH_ARM)
    add_compile_options(-march=armv8-a+crc)
endif()
#

if (WITH_MYSQL)
    add_definitions(-DDORIS_WITH_MYSQL)
endif()

if (ENABLE_INJECTION_POINT)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DENABLE_INJECTION_POINT")
endif()

# Enable memory tracker, which allows BE to limit the memory of tasks such as query, load,
# and compaction,and observe the memory of BE through be_ip:http_port/MemTracker.
# Adding the option `USE_MEM_TRACKER=OFF sh build.sh` when compiling can turn off the memory tracker,
# which will bring about a 2% performance improvement, which may be useful in performance POC.
if (USE_MEM_TRACKER)
    add_definitions(-DUSE_MEM_TRACKER)
endif()

# Compile with jemalloc.
# Adding the option `USE_JEMALLOC=ON sh build.sh` when compiling can turn on building with jemalloc
if (USE_JEMALLOC)
    add_definitions(-DUSE_JEMALLOC)
endif()

# Compile with libunwind
if (USE_UNWIND)
    add_definitions(-DUSE_UNWIND)
    if (COMPILER_CLANG)
        add_compile_options(-gdwarf-aranges)
    endif()
endif()

if (USE_DWARF)
    add_compile_options(-gdwarf-5)
endif()

# For CMAKE_BUILD_TYPE=Debug
if (OS_MACOSX AND ARCH_ARM)
    # Using -O0 may meet ARM64 branch out of range errors when linking with tcmalloc.
    set(CXX_FLAGS_DEBUG "-Og")
else()
    set(CXX_FLAGS_DEBUG "-O0")
endif()

# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
set(CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CXX_FLAGS_ASAN "-O0 -fsanitize=address -fsanitize=undefined -fno-strict-aliasing -fno-sanitize=alignment,signed-integer-overflow,float-cast-overflow -DUNDEFINED_BEHAVIOR_SANITIZER -DADDRESS_SANITIZER")
set(CXX_FLAGS_LSAN "-O0 -fsanitize=leak -DLEAK_SANITIZER")
## Use for BE-UT
set(CXX_FLAGS_ASAN_UT "-O0 -fsanitize=address -DADDRESS_SANITIZER")

# Set the flags to the undefined behavior sanitizer, also known as "ubsan"
# Turn on sanitizer and debug symbols to get stack traces:
set(CXX_FLAGS_UBSAN "-O0 -fno-wrapv -mcmodel=medium -fsanitize=undefined -DUNDEFINED_BEHAVIOR_SANITIZER")

# Set the flags to the thread sanitizer, also known as "tsan"
# Turn on sanitizer and debug symbols to get stack traces:
# Use -Wno-builtin-declaration-mismatch to mute warnings like "new declaration ‘__tsan_atomic16 __tsan_atomic16_fetch_nand(..."
# If use -O0 to compile, BE will stack overflow when start. https://github.com/apache/doris/issues/8868
set(CXX_FLAGS_TSAN "-O1 -fsanitize=thread -DTHREAD_SANITIZER -Wno-missing-declarations")

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_ASAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_LSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_UBSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_TSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN_UT")
    set(CMAKE_CXX_FLAGS "${CXX_FLAGS_ASAN_UT}")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# Add flags that are common across build types
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS} ${EXTRA_CXX_FLAGS}")

set(CMAKE_C_FLAGS ${CMAKE_CXX_FLAGS})

# Set include dirs
include_directories(
    ${SRC_DIR}/apache-orc/c++/include
    ${CMAKE_CURRENT_BINARY_DIR}/src/apache-orc/c++/include
)

include_directories(
    ${CMAKE_CURRENT_BINARY_DIR}/src/clucene/src/shared
    ${SRC_DIR}/clucene/src/core
    ${SRC_DIR}/clucene/src/shared
    ${SRC_DIR}/clucene/src/contribs-lib
)

include_directories(
    ${SRC_DIR}/
    ${TEST_DIR}/
)

include_directories(
    SYSTEM
    ${COMMON_SRC_DIR}
    ${GENSRC_DIR}/
    ${THIRDPARTY_DIR}/include
    ${GPERFTOOLS_HOME}/include
)

if ("${DORIS_JAVA_HOME}" STREQUAL "")
    set(DORIS_JAVA_HOME "$ENV{JAVA_HOME}")
endif()

execute_process(COMMAND chmod 755 ${BASE_DIR}/../tools/find_libjvm.sh)
execute_process(COMMAND ${BASE_DIR}/../tools/find_libjvm.sh OUTPUT_VARIABLE LIBJVM_PATH OUTPUT_STRIP_TRAILING_WHITESPACE)
FILE(GLOB_RECURSE LIB_JVM ${LIBJVM_PATH})
if("${LIB_JVM}" STREQUAL "")
    message(STATUS "there is no libjvm found!")
else()
    set(DORIS_DEPENDENCIES
        ${DORIS_DEPENDENCIES}
        jvm
    )
    add_library(jvm SHARED IMPORTED)
    set_target_properties(jvm PROPERTIES IMPORTED_LOCATION ${LIB_JVM})
    include_directories(${DORIS_JAVA_HOME}/include)
    if (NOT OS_MACOSX)
        include_directories(${DORIS_JAVA_HOME}/include/linux)
    else()
        include_directories(${DORIS_JAVA_HOME}/include/darwin)
    endif()
    add_definitions("-DLIBJVM")
endif()

if (NOT OS_MACOSX)
    set(WL_START_GROUP "-Wl,--start-group")
    set(WL_END_GROUP "-Wl,--end-group")
endif()

set(KRB5_LIBS
    krb5support
    krb5
    com_err
    gssapi_krb5
    k5crypto)

# Set Doris libraries
set(DORIS_LINK_LIBS
    ${WL_START_GROUP}
    Agent
    Common
    Exec
    Exprs
    Gutil
    IO
    Olap
    Runtime
    Service
    Udf
    Util
    DorisGen
    Webserver
    Geo
    GeoType
    Vec
    Pipeline
    Cloud
    ${WL_END_GROUP}
    CommonCPP
)

set(absl_DIR ${THIRDPARTY_DIR}/lib/cmake/absl)
find_package(absl)

# COMMON_THIRDPARTY are thirdparty dependencies that can run on all platform
# When adding new dependencies, If you don’t know if it can run on all platforms,
# add it here first.
set(COMMON_THIRDPARTY
    Boost::date_time
    Boost::container
    ${COMMON_THIRDPARTY}
)

if ((ARCH_AMD64 OR ARCH_AARCH64) AND OS_LINUX)
    add_library(hadoop_hdfs STATIC IMPORTED)
    set_target_properties(hadoop_hdfs PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/hadoop_hdfs/native/libhdfs.a)

    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        hadoop_hdfs
    )
    add_definitions(-DUSE_HADOOP_HDFS)
else()
    add_library(hdfs3 STATIC IMPORTED)
    set_target_properties(hdfs3 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libhdfs3.a)

    # TODO: use arm hadoop hdfs to replace this
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        hdfs3
    )
    add_definitions(-DUSE_LIBHDFS3)
endif()

if (absl_FOUND)
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        absl::flags_internal
        absl::flags_reflection
        absl::random_internal_pool_urbg
        absl::random_internal_randen
        absl::random_internal_seed_material
        absl::spinlock_wait
        absl::status
        absl::statusor
        absl::strings
    )
endif()

if (OS_MACOSX)
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        bfd
        iberty
        intl
    )
endif()

if (MAKE_TEST)
    set(COMMON_THIRDPARTY
        ${COMMON_THIRDPARTY}
        benchmark
    )
endif()

set(DORIS_DEPENDENCIES
    ${DORIS_DEPENDENCIES}
    ${WL_START_GROUP}
    ${COMMON_THIRDPARTY}
    ${KRB5_LIBS}
)

if (WITH_MYSQL)
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} mysql)
endif()

if (USE_UNWIND)
    set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} libunwind)
endif()

set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} orc)
set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} ic)
set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} clucene-core-static)
set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} clucene-shared-static)
set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} clucene-contribs-lib)

set(DORIS_DEPENDENCIES ${DORIS_DEPENDENCIES} ${WL_END_GROUP})

# Add all external dependencies. They should come after the palo libs.
# static link gcc's lib
if (NOT OS_MACOSX)
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
        ${DORIS_DEPENDENCIES}
        -static-libstdc++
        -static-libgcc
        -lresolv
    )
    if (NOT (USE_LIBCPP AND COMPILER_CLANG))
        set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} -lstdc++fs)
    endif()
else()
    set(DORIS_LINK_LIBS
        ${DORIS_LINK_LIBS}
        ${DORIS_DEPENDENCIES}
        -lapple_nghttp2
        -lresolv
        -liconv
    )
endif()


if (USE_JEMALLOC)
    set(MALLOCLIB jemalloc)
else ()
    set(MALLOCLIB tcmalloc)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(ASAN_LIBS -static-libasan)
    set(LSAN_LIBS -static-liblsan)
    set(UBSAN_LIBS -static-libubsan ${MALLOCLIB})
    set(TSAN_LIBS -static-libtsan)
else ()
    set(UBSAN_LIBS -rtlib=compiler-rt ${MALLOCLIB})
endif ()

# Add sanitize static link flags
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${MALLOCLIB})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN" OR "${CMAKE_BUILD_TYPE}" STREQUAL "ASAN_UT")
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

if (NOT OS_MACOSX)
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
        -lrt -l:libbfd.a -liberty -lc -lm -ldl -pthread
    )
else()
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreText"
        "-framework Foundation"
        "-framework SystemConfiguration"
        "-framework Security"
    )
    if (USE_JEMALLOC OR (NOT CMAKE_BUILD_TYPE STREQUAL "DEBUG" AND NOT CMAKE_BUILD_TYPE STREQUAL "RELEASE"))
        set(DORIS_LINK_LIBS
            ${DORIS_LINK_LIBS}
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        )
    endif()
endif()

# Set libraries for test
set (TEST_LINK_LIBS ${DORIS_LINK_LIBS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

# Only build static libs
set(BUILD_SHARED_LIBS OFF)

option(ENABLE_CLANG_COVERAGE "coverage option" OFF)
if (ENABLE_CLANG_COVERAGE AND ENABLE_CLANG_COVERAGE STREQUAL ON AND COMPILER_CLANG)
    add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
    add_link_options(-fprofile-instr-generate)
endif ()

if (MAKE_TEST)
    add_compile_options(-fprofile-arcs -ftest-coverage -DGTEST_USE_OWN_TR1_TUPLE=0)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
    if (NOT OS_MACOSX)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lgcov")
    endif()
    add_definitions(-DBE_TEST)
    if (ARCH_ARM)
        add_compile_options(-ffp-contract=off)
    endif()
endif ()

get_directory_property(COMPILER_FLAGS COMPILE_OPTIONS)
get_directory_property(COMPILER_DEFINES COMPILE_DEFINITIONS)
message(STATUS "Compiler: ${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "CXX Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "C Standard: ${CMAKE_C_STANDARD}")
message(STATUS "CXX Flags: ${CMAKE_CXX_FLAGS}")
message(STATUS "C Flags: ${CMAKE_C_FLAGS}")
message(STATUS "CC Flags: ${CMAKE_CC_FLAGS}")
message(STATUS "Compiler Options: ${COMPILER_FLAGS}")
message(STATUS "Compiler Definitions: ${COMPILER_DEFINES}")
message(STATUS "Doris Dependencies: ${DORIS_DEPENDENCIES}")
if (NOT MAKE_TEST)
    message(STATUS "Link Flags: ${DORIS_LINK_LIBS}")
else()
    message(STATUS "Link Flags: ${TEST_LINK_LIBS}")
endif()

if (ENABLE_PCH)
    add_library(pch STATIC ${SRC_DIR}pch/pch.cc)
    target_precompile_headers(
        pch
        PUBLIC
            ${SRC_DIR}pch/pch.h
    )
    if (COMPILER_CLANG)
        target_compile_options(pch PRIVATE -Xclang -fno-pch-timestamp)
    endif()
endif()

function(pch_reuse target)
    if (ENABLE_PCH)
        target_precompile_headers(${target} REUSE_FROM pch)
    endif()
endfunction(pch_reuse target)

add_subdirectory(${SRC_DIR}/agent)
add_subdirectory(${SRC_DIR}/common)
add_subdirectory(${SRC_DIR}/exec)
add_subdirectory(${SRC_DIR}/exprs)
add_subdirectory(${SRC_DIR}/gen_cpp)
add_subdirectory(${SRC_DIR}/geo)
add_subdirectory(${SRC_DIR}/gutil)
add_subdirectory(${SRC_DIR}/http)
add_subdirectory(${SRC_DIR}/io)
add_subdirectory(${SRC_DIR}/olap)
add_subdirectory(${SRC_DIR}/runtime)
add_subdirectory(${SRC_DIR}/service)
add_subdirectory(${SRC_DIR}/udf)
add_subdirectory(${SRC_DIR}/cloud)

option(BUILD_META_TOOL "Build meta tool" OFF)
if (BUILD_META_TOOL)
    add_subdirectory(${SRC_DIR}/tools)
endif()

option(BUILD_INDEX_TOOL "Build index tool" OFF)
if (BUILD_INDEX_TOOL)
    add_subdirectory(${SRC_DIR}/index-tools)
endif()

add_subdirectory(${SRC_DIR}/util)
add_subdirectory(${SRC_DIR}/vec)
add_subdirectory(${SRC_DIR}/pipeline)

if (MAKE_TEST)
    add_subdirectory(${TEST_DIR})
endif ()

add_subdirectory(${COMMON_SRC_DIR}/cpp ${BUILD_DIR}/src/common_cpp)

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
    ${BASE_DIR}/../conf/asan_suppr.conf
    ${BASE_DIR}/../conf/lsan_suppr.conf
    DESTINATION ${OUTPUT_DIR}/conf)

get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach(dir ${dirs})
    message(STATUS "dir='${dir}'")
endforeach()


