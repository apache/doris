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

project(doris_cloud CXX C)

# Write compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Sanitizer builds (ASAN/MSAN/TSAN/UBSAN/LSAN) cannot use prebuilt thirdparty —
# the prebuilt libs are not instrumented. BE forces USE_CONTRIB_SOURCE=ON for
# those build types; cloud must do the same so its include_directories block
# below picks the source-mode paths.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "ASAN_UT" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "TSAN" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "MSAN" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(USE_CONTRIB_SOURCE ON CACHE BOOL "Build thirdparty from source" FORCE)
    message(STATUS "Cloud sanitizer build (${CMAKE_BUILD_TYPE}): forcing USE_CONTRIB_SOURCE=ON")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(cppcheck)

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
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
endif ()

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
set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}")
if (NOT DEFINED ENV{DORIS_BRANCH})
    set(THIRDPARTY_DIR "$ENV{DORIS_THIRDPARTY}/installed")
else()
    set(THIRDPARTY_DIR "$ENV{DORIS_THIRDPARTY}/installed-$ENV{DORIS_BRANCH}")
endif()
set(GENSRC_DIR "${BASE_DIR}/../gensrc/build/")
set(COMMON_SRC_DIR "${BASE_DIR}/../common")
set(SRC_DIR "${BASE_DIR}/src/")
set(TEST_DIR "${CMAKE_SOURCE_DIR}/test/")
set(OUTPUT_DIR "${BASE_DIR}/output")
set(THIRDPARTY_SRC "$ENV{DORIS_THIRDPARTY}/src")

if (APPLE)
    set(MAKE_TEST "ON")
else()
    option(MAKE_TEST "ON for make unit test or OFF for not" OFF)
endif()
message(STATUS "make test: ${MAKE_TEST}")

# Check gcc
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "11.0")
        message(FATAL_ERROR "Need GCC version at least 11.0")
    endif()
endif()

set(CMAKE_SKIP_RPATH TRUE)

# Compile generated source if necessary
message(STATUS "build gensrc if necessary")
execute_process(COMMAND make -C ${BASE_DIR}/../gensrc/
                RESULT_VARIABLE MAKE_GENSRC_RESULT)
if(NOT ${MAKE_GENSRC_RESULT} EQUAL 0 AND NOT APPLE)
    message(FATAL_ERROR "Failed to build ${BASE_DIR}/../gensrc/")
endif()

set(GPERFTOOLS_HOME "${THIRDPARTY_DIR}/gperftools")

# Set all libraries
include(thirdparty)

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
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -std=gnu++20 -D__STDC_FORMAT_MACROS")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_SYSTEM_NO_DEPRECATED")
# Enable the cpu and heap profile of brpc
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBRPC_ENABLE_CPU_PROFILER")

function(TRY_TO_CHANGE_LINKER LINKER_COMMAND LINKER_NAME)
    if (CUSTUM_LINKER_COMMAND STREQUAL "ld")
        execute_process(COMMAND ${CMAKE_C_COMPILER} -fuse-ld=${LINKER_COMMAND} -Wl,--version ERROR_QUIET OUTPUT_VARIABLE LD_VERSION)
        if ("${LD_VERSION}" MATCHES ${LINKER_NAME})
            message("Linker ${LINKER_NAME} is available, change linker to ${LINKER_NAME}")
            set(CUSTUM_LINKER_COMMAND "${LINKER_COMMAND}" PARENT_SCOPE)
        endif()
    endif()
endfunction()

# In terms of performance, mold> lld> gold> ld
set(CUSTUM_LINKER_COMMAND "ld")
# TODO: mold will link fail on thirdparty brpc now, waiting for investigation.
# TRY_TO_CHANGE_LINKER("mold" "mold")
TRY_TO_CHANGE_LINKER("lld" "LLD")
TRY_TO_CHANGE_LINKER("gold" "GNU gold")
if (NOT CUSTUM_LINKER_COMMAND STREQUAL "ld")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${CUSTUM_LINKER_COMMAND}")
endif()

if ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
    set(USE_LIBCPP ON)
    set(USE_JEMALLOC OFF)
endif()

if (USE_LIBCPP AND COMPILER_CLANG)
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
        # MSAN: use instrumented libc++ built from source
        set(_MSAN_LIBCXX_DIR "$ENV{DORIS_THIRDPARTY}/build_msan_libcxx")
        if (EXISTS "${_MSAN_LIBCXX_DIR}/lib/libc++.a")
            message(STATUS "MSAN: using instrumented libc++ from ${_MSAN_LIBCXX_DIR}")
            set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -nostdinc++ -isystem ${_MSAN_LIBCXX_DIR}/include/c++/v1")
            set(MSAN_LIBCXX_LIBS
                "${_MSAN_LIBCXX_DIR}/lib/libc++.a"
                "${_MSAN_LIBCXX_DIR}/lib/libc++abi.a"
                "${_MSAN_LIBCXX_DIR}/lib/libunwind.a")
        else()
            message(WARNING "MSAN: instrumented libc++ not found, falling back to toolchain libc++")
            set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libc++")
        endif()
    else()
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libc++")
    endif()
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

# Compile with jemalloc.
# Adding the option `USE_JEMALLOC=ON sh build.sh` when compiling can turn on building with jemalloc 
if (USE_JEMALLOC)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DUSE_JEMALLOC")
endif()

if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 7.0)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -faligned-new")
endif()

if(BUILD_CHECK_META STREQUAL "ON")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBUILD_CHECK_META")
endif()

# For any gcc builds:
#   -g: Enable symbols for profiler tools. Produce debugging information in the operating system’s native formt
#   -Wno-unused-local-typedefs: Do not warn for local typedefs that are unused.
set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -ggdb -Wno-unused-local-typedefs -gdwarf-5")

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
# If use -O0 to compile, process may stack overflow when start. https://github.com/apache/doris/issues/8868
SET(CXX_FLAGS_TSAN "${CXX_GCC_FLAGS} -O1 -fsanitize=thread -DTHREAD_SANITIZER -Wno-missing-declarations")

# MSAN flags — must use instrumented libc++ (set above)
SET(CXX_FLAGS_MSAN "${CXX_GCC_FLAGS} -O1 -fsanitize=memory -fsanitize-memory-track-origins=2 -fsanitize-ignorelist=$ENV{DORIS_HOME}/conf/msan_ignorelist.txt -fno-omit-frame-pointer -DMEMORY_SANITIZER")

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
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_MSAN}")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# Add flags that are common across build types
SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS} ${EXTRA_CXX_FLAGS}")

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
    ${COMMON_SRC_DIR}
    ${THIRDPARTY_DIR}/include
    ${GPERFTOOLS_HOME}/include
)

# Source-mode (USE_CONTRIB_SOURCE=ON): ${THIRDPARTY_DIR}/include is empty —
# headers live under thirdparty/src/<lib>/ and the build tree. Mirror the
# subset BE's CMakeLists.txt exposes so cloud/src/ can find fmt, brpc's
# generated butil/config.h, aws-cpp-sdk-core (cJSON, SDKConfig.h), etc.
if (USE_CONTRIB_SOURCE)
    # cloud's CMakeLists.txt is invoked as a separate top-level project rooted
    # at ${DORIS_HOME}/cloud/, so CMAKE_SOURCE_DIR points there; go up one to
    # reach thirdparty/. BE's source-mode build dir is be/build_<TYPE>/thirdparty,
    # while cloud has its own build_<TYPE> tree — point at BE's so we reuse the
    # already-generated headers (configure_file outputs from aws_sdk.cmake,
    # brpc, jemalloc, etc.).
    set(_CLOUD_TP_SRC "${CMAKE_SOURCE_DIR}/../thirdparty/src")
    set(_CLOUD_TP_BUILD "${CMAKE_SOURCE_DIR}/../be/build_${CMAKE_BUILD_TYPE}/thirdparty")
    include_directories(SYSTEM
        ${_CLOUD_TP_SRC}/fmt-8.1.1/include
        ${_CLOUD_TP_SRC}/brpc-1.4.0/src
        ${_CLOUD_TP_BUILD}/brpc
        ${_CLOUD_TP_SRC}/glog-0.6.0/src
        ${_CLOUD_TP_BUILD}/glog
        ${_CLOUD_TP_SRC}/protobuf-21.11/src
        ${_CLOUD_TP_SRC}/aws-sdk-cpp-1.11.219/src/aws-cpp-sdk-core/include
        ${_CLOUD_TP_BUILD}/aws-sdk-gen/aws-cpp-sdk-core/include
        ${_CLOUD_TP_SRC}/aws-sdk-cpp-1.11.219/generated/src/aws-cpp-sdk-s3/include
        ${_CLOUD_TP_SRC}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/include
        ${_CLOUD_TP_SRC}/aws-sdk-cpp-1.11.219/crt/aws-crt-cpp/crt/aws-c-common/include
        ${_CLOUD_TP_SRC}/boost_1_81_0
        ${_CLOUD_TP_SRC}/abseil-cpp-20250512.1
        ${_CLOUD_TP_SRC}/rapidjson-232389d4f1012dddec4ef84861face2d2ba85709/include
        ${_CLOUD_TP_SRC}/jemalloc-5.3.0/include
        ${_CLOUD_TP_BUILD}/jemalloc_config
        ${_CLOUD_TP_BUILD}/thrift_gen
        ${_CLOUD_TP_SRC}/thrift-0.16.0/lib/cpp/src
        # azure-sdk-for-cpp: cloud/src/recycler uses azure/core, azure/storage/blobs,
        # azure/storage/common; the identity path mirrors BE for symmetry.
        ${_CLOUD_TP_SRC}/azure-sdk-for-cpp-azure-core_1.16.0/sdk/core/azure-core/inc
        ${_CLOUD_TP_SRC}/azure-sdk-for-cpp-azure-core_1.16.0/sdk/identity/azure-identity/inc
        ${_CLOUD_TP_SRC}/azure-sdk-for-cpp-azure-core_1.16.0/sdk/storage/azure-storage-blobs/inc
        ${_CLOUD_TP_SRC}/azure-sdk-for-cpp-azure-core_1.16.0/sdk/storage/azure-storage-common/inc
    )
endif()

option(ENABLE_HDFS_STORAGE_VAULT "Enable HDFS storage support" ON)
if (ENABLE_HDFS_STORAGE_VAULT)
    add_compile_definitions(ENABLE_HDFS_STORAGE_VAULT)
    if ("${DORIS_JAVA_HOME}" STREQUAL "")
        set(DORIS_JAVA_HOME "$ENV{JAVA_HOME}")
    endif()

    include_directories(${DORIS_JAVA_HOME}/include)
    if (NOT OS_MACOSX)
        include_directories(${DORIS_JAVA_HOME}/include/linux)
    else()
        include_directories(${DORIS_JAVA_HOME}/include/darwin)
    endif()
endif()

set(WL_START_GROUP "-Wl,--start-group")
set(WL_END_GROUP "-Wl,--end-group")

# Set Doris libraries
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

set(DORIS_DEPENDENCIES
    ${DORIS_DEPENDENCIES}
    ${WL_START_GROUP}
    ${COMMON_THIRDPARTY}
    ${WL_END_GROUP}
)

message(STATUS "DORIS_DEPENDENCIES is ${DORIS_DEPENDENCIES}")

# Add all external dependencies. They should come after the project's libs.
# static link gcc's lib
set(DORIS_LINK_LIBS
    CommonCPP
    ${DORIS_DEPENDENCIES}
    -static-libstdc++
    -static-libgcc
    -lresolv
)

if (ENABLE_HDFS_STORAGE_VAULT)
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
        -L${DORIS_JAVA_HOME}/lib/server
        -ljvm)
endif()

if (NOT (USE_LIBCPP AND COMPILER_CLANG))
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} -lstdc++fs)
endif()

if (USE_JEMALLOC)
    set(MALLOCLIB jemalloc)
else ()
    set(MALLOCLIB tcmalloc)
endif()

option(BUILD_AZURE "ON for building azure support for BE or OFF for not" OFF)
message(STATUS "build azure: ${BUILD_AZURE}")
if(BUILD_AZURE STREQUAL "ON")
    add_definitions(-DUSE_AZURE)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(ASAN_LIBS -static-libasan)
    set(LSAN_LIBS -static-liblsan)
    set(UBSAN_LIBS -static-libubsan ${MALLOCLIB})
    set(TSAN_LIBS -static-libtsan)
else ()
    set(UBSAN_LIBS -rtlib=compiler-rt ${MALLOCLIB})
endif ()

# Add sanitize static link flags or tcmalloc
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${MALLOCLIB})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${ASAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${LSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${UBSAN_LIBS})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} ${TSAN_LIBS})
    add_definitions("-DTHREAD_SANITIZER")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN")
    # MSAN: no extra malloc lib, clang links sanitizer runtime automatically
    add_definitions("-DMEMORY_SANITIZER")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS}
    -lrt -l:libbfd.a -liberty -lc -lm -ldl -pthread
)

if(BUILD_CHECK_META STREQUAL "ON")
    set(DORIS_LINK_LIBS ${DORIS_LINK_LIBS} -lmysqlclient)
endif ()

# Only build static libs
set(BUILD_SHARED_LIBS OFF)

option(ENABLE_CLANG_COVERAGE "coverage option" OFF)
if (ENABLE_CLANG_COVERAGE AND ENABLE_CLANG_COVERAGE STREQUAL ON AND COMPILER_CLANG)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
endif ()

if (${MAKE_TEST} STREQUAL "ON")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUNIT_TEST -fprofile-arcs -ftest-coverage -fno-access-control -DGTEST_USE_OWN_TR1_TUPLE=0")
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -lgcov")
    add_definitions(-DBE_TEST)
endif ()

if (ENABLE_INJECTION_POINT)
    add_definitions(-DENABLE_INJECTION_POINT)
endif()

# enable glog custom prefix
add_definitions(-DGLOG_CUSTOM_PREFIX_SUPPORT)

set(FDB_VERSIONS "7.1.57" "7.3.69")
set(FDB_DEFAULT_VERSION "7.1.57")
include(install_fdb)
# --allow-multiple-definition: aws-c-io's tls_channel_handler.c and s2n_tls_channel_handler.c
# both define aws_tls_key_operation_* (upstream design for GCC, needs explicit flag for lld)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lfdb_c -L${THIRDPARTY_DIR}/lib -Wl,--allow-multiple-definition")

set(DORIS_CLOUD_LIBS
    CloudGen
    Common
    MetaService
    MetaStore
    ResourceManager
    Recycler
    RateLimiter
    Snapshot
)

if (EXISTS ${SRC_DIR}/enterprise/CMakeLists.txt)
    add_subdirectory(${SRC_DIR}/enterprise)
    if (DORIS_ADDITIONAL_DEFINITIONS)
        add_definitions(${DORIS_ADDITIONAL_DEFINITIONS})
    endif()
endif()

add_subdirectory(${SRC_DIR}/common)
add_subdirectory(${SRC_DIR}/gen-cpp)
add_subdirectory(${SRC_DIR}/meta-service)
add_subdirectory(${SRC_DIR}/meta-store)
add_subdirectory(${SRC_DIR}/recycler)
add_subdirectory(${SRC_DIR}/rate-limiter)
add_subdirectory(${SRC_DIR}/resource-manager)
add_subdirectory(${SRC_DIR}/snapshot)

# Set libraries for doris_cloud
set(DORIS_LINK_LIBS
    ${WL_START_GROUP}
    ${DORIS_CLOUD_LIBS}
    ${WL_END_GROUP}
    ${DORIS_LINK_LIBS})

# Set libraries for test
set (TEST_LINK_LIBS ${DORIS_LINK_LIBS}
    ${WL_START_GROUP}
    gmock
    gtest
    gtest_main
    ${WL_END_GROUP}
)

if (${MAKE_TEST} STREQUAL "ON")
    add_subdirectory(${TEST_DIR})
endif ()

add_subdirectory(${COMMON_SRC_DIR}/cpp ${BUILD_DIR}/src/common_cpp)

if (${MAKE_TEST} STREQUAL "OFF")
    add_executable(doris_cloud src/main.cpp)

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    set_target_properties(doris_cloud PROPERTIES ENABLE_EXPORTS 1)
    target_link_libraries(doris_cloud ${DORIS_LINK_LIBS})

    if ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN" AND MSAN_LIBCXX_LIBS)
        target_link_libraries(doris_cloud -nostdlib++ ${MSAN_LIBCXX_LIBS})
    endif()

    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib)
    install(TARGETS doris_cloud DESTINATION ${OUTPUT_DIR}/lib)

    if ("${STRIP_DEBUG_INFO}" STREQUAL "ON")
        add_custom_command(TARGET doris_cloud POST_BUILD
            COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:doris_cloud> $<TARGET_FILE:doris_cloud>.dbg
            COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:doris_cloud>
            COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:doris_cloud>.dbg $<TARGET_FILE:doris_cloud>
            )

        install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
        install(FILES $<TARGET_FILE:doris_cloud>.dbg DESTINATION ${OUTPUT_DIR}/lib/debug_info/)
    endif()
endif()


message("install files at ${OUTPUT_DIR}")
install(DIRECTORY DESTINATION ${OUTPUT_DIR})
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/bin)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/conf)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib)

install(FILES
    ${BASE_DIR}/script/start.sh
    ${BASE_DIR}/script/stop.sh
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${OUTPUT_DIR}/bin)

install(FILES
    ${BASE_DIR}/conf/doris_cloud.conf
    ${BASE_DIR}/conf/asan_suppr.conf
    ${BASE_DIR}/conf/lsan_suppr.conf
    ${BASE_DIR}/conf/ubsan_suppr.conf
    DESTINATION ${OUTPUT_DIR}/conf)

install(FILES
    ${THIRDPARTY_DIR}/lib/libfdb_c.so
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${OUTPUT_DIR}/lib)

foreach(version ${FDB_VERSIONS})
    if (version STREQUAL ${FDB_DEFAULT_VERSION})
        continue()
    endif()

    string(REPLACE "." "_" version_underscored ${version})
    install(FILES
        ${FDB_INSTALL_DIR_${version_underscored}}/lib64/libfdb_c.so
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_WRITE GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
        DESTINATION ${OUTPUT_DIR}/lib/fdb/${version})
endforeach()
