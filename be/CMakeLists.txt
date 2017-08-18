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

cmake_minimum_required(VERSION 2.6.0)
project(palo)

# Set dirs
set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(ENV{PALO_HOME} "${BASE_DIR}/../")
set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(THIRDPARTY_DIR "${BASE_DIR}/../thirdparty/installed/")
set(GENSRC_DIR "${BASE_DIR}/../gensrc/build/")
set(SRC_DIR "${BASE_DIR}/src/")
set(TEST_DIR "${CMAKE_SOURCE_DIR}/test/")
set(OUTPUT_DIR "${BASE_DIR}/output")

option(MAKE_TEST "ON for make unit test or OFF for not" OFF)

# Check gcc
if (CMAKE_COMPILER_IS_GNUCC)
    execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion
                    OUTPUT_VARIABLE GCC_VERSION)
    string(REGEX MATCHALL "[0-9]+" GCC_VERSION_COMPONENTS ${GCC_VERSION})
    list(GET GCC_VERSION_COMPONENTS 0 GCC_MAJOR)
    list(GET GCC_VERSION_COMPONENTS 1 GCC_MINOR)

    message(STATUS "GCC version: ${GCC_VERSION}")
    message(STATUS "GCC major version: ${GCC_MAJOR}")
    message(STATUS "GCC minor version: ${GCC_MINOR}")

    if(GCC_VERSION VERSION_LESS "4.8.2")
        message(FATAL_ERROR "Need GCC version at least 4.8.2")
    endif(GCC_VERSION VERSION_LESS "4.8.2")
else()
    message(FATAL_ERROR "Compiler should be GNU")
endif(CMAKE_COMPILER_IS_GNUCC)

# Set compiler
set(CMAKE_CXX_COMPILER $ENV{CXX})
set(CMAKE_C_COMPILER $ENV{CC})

set(PIC_LIB_PATH "${THIRDPARTY_DIR}")
if(PIC_LIB_PATH)
    message(STATUS "defined PIC_LIB_PATH")
    set(CMAKE_SKIP_RPATH TRUE)
    set(Boost_USE_STATIC_LIBS ON)
    set(Boost_USE_STATIC_RUNTIME ON)
    set(LIBBZ2 ${PIC_LIB_PATH}/lib/libbz2.a)
    set(LIBZ ${PIC_LIB_PATH}/lib/libz.a)
    set(LIBEVENT ${PIC_LIB_PATH}/lib/libevent.a)
else()
    message(STATUS "undefined PIC_LIB_PATH")
    set(Boost_USE_STATIC_LIBS ON)
    set(Boost_USE_STATIC_RUNTIME ON)
    set(LIBBZ2 -lbz2)
    set(LIBZ -lz)
    set(LIBEVENT event)
endif()

# Compile generated source if necessary
message(STATUS "build gensrc if necessary")
execute_process(COMMAND make -C ${BASE_DIR}/../gensrc/
                RESULT_VARIABLE MAKE_GENSRC_RESULT)
if(NOT ${MAKE_GENSRC_RESULT} EQUAL 0)
    message(FATAL_ERROR "Failed to build ${BASE_DIR}/../gensrc/")
endif()

# Set Boost
set(Boost_DEBUG FALSE)
set(Boost_USE_MULTITHREADED ON)
set(BOOST_ROOT ${THIRDPARTY_DIR})

find_package(Boost 1.55.0 REQUIRED COMPONENTS thread regex system filesystem date_time program_options)
include_directories(${Boost_INCLUDE_DIRS})

# Set all libraries
add_library(gflags STATIC IMPORTED)
set_target_properties(gflags PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgflags.a)

add_library(glog STATIC IMPORTED)
set_target_properties(glog PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libglog.a)

add_library(re2 STATIC IMPORTED)
set_target_properties(re2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libre2.a)

add_library(pprof STATIC IMPORTED)
set_target_properties(pprof PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprofiler.a)

add_library(tcmalloc STATIC IMPORTED)
set_target_properties(tcmalloc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libtcmalloc.a)

add_library(unwind STATIC IMPORTED)
set_target_properties(unwind PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libunwind.a)

add_library(protobuf STATIC IMPORTED)
set_target_properties(protobuf PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotobuf.a)

add_library(protoc STATIC IMPORTED)
set_target_properties(protoc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotoc.a)

add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgtest.a)

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

add_library(lzo STATIC IMPORTED)
set_target_properties(lzo PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/liblzo2.a)

add_library(mysql STATIC IMPORTED)
set_target_properties(mysql PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libmysqlclient.a)

add_library(libevent STATIC IMPORTED)
set_target_properties(libevent PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libevent.a)

add_library(LLVMSupport STATIC IMPORTED)
set_target_properties(LLVMSupport PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libLLVMSupport.a)

add_library(crypto STATIC IMPORTED)
set_target_properties(crypto PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcrypto.a)

add_library(openssl STATIC IMPORTED)
set_target_properties(openssl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libssl.a)

find_program(THRIFT_COMPILER thrift ${CMAKE_SOURCE_DIR}/bin)

# LLVM
set(LLVM_BIN "${THIRDPARTY_DIR}/bin")
message(STATUS ${LLVM_HOME})

# llvm-config
find_program(LLVM_CONFIG_EXECUTABLE llvm-config
  PATHS
  ${LLVM_BIN}
  NO_DEFAULT_PATH
)

if (NOT LLVM_CONFIG_EXECUTABLE)
  message(FATAL_ERROR "Could not find llvm-config")
endif (NOT LLVM_CONFIG_EXECUTABLE)

# clang++
find_program(LLVM_CLANG_EXECUTABLE clang++
  PATHS
  ${LLVM_BIN}
  NO_DEFAULT_PATH
)

if (NOT LLVM_CLANG_EXECUTABLE)
  message(FATAL_ERROR "Could not find clang++")
endif (NOT LLVM_CLANG_EXECUTABLE)

# opt
find_program(LLVM_OPT_EXECUTABLE opt
  PATHS
  ${LLVM_BIN}
  NO_DEFAULT_PATH
)

if (NOT LLVM_OPT_EXECUTABLE)
  message(FATAL_ERROR "Could not find llvm opt")
endif (NOT LLVM_OPT_EXECUTABLE)

message(STATUS "LLVM llvm-config found at: ${LLVM_CONFIG_EXECUTABLE}")
message(STATUS "LLVM clang++ found at: ${LLVM_CLANG_EXECUTABLE}")
message(STATUS "LLVM opt found at: ${LLVM_OPT_EXECUTABLE}")

# Get all llvm depends
execute_process(
  COMMAND ${LLVM_CONFIG_EXECUTABLE} --includedir
  OUTPUT_VARIABLE LLVM_INCLUDE_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
  COMMAND ${LLVM_CONFIG_EXECUTABLE} --libdir
  OUTPUT_VARIABLE LLVM_LIBRARY_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
  COMMAND ${LLVM_CONFIG_EXECUTABLE} --ldflags
  OUTPUT_VARIABLE LLVM_LFLAGS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Get the link libs we need.  llvm has many and we don't want to link all of the libs
# if we don't need them.   
execute_process(
  COMMAND ${LLVM_CONFIG_EXECUTABLE} --libnames core jit native ipo bitreader target
  OUTPUT_VARIABLE LLVM_MODULE_LIBS
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# TODO: this does not work well.  the config file will output -I/<include path> and
# also -DNDEBUG.  I've hard coded the #define that are necessary but we should make
# this better.  The necesesary flags are only #defines so maybe just def/undef those
# around #include to llvm headers?
#execute_process(
#  COMMAND ${LLVM_CONFIG_EXECUTABLE} --cppflags
#  OUTPUT_VARIABLE LLVM_CFLAGS
#  OUTPUT_STRIP_TRAILING_WHITESPACE
#)
set(LLVM_CFLAGS 
    "-D_GNU_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS")
if(GCC_VERSION VERSION_LESS "5.0.0")
    message(STATUS "GCC version is less than 5.0.0, no need to set -D__GLIBCXX_BITSIZE_INT_N_0=128 and -D__GLIBCXX_TYPE_INT_N_0=__int128")
else()
    SET(LLVM_CFLAGS "${LLVM_LFLAGS} -D__GLIBCXX_BITSIZE_INT_N_0=128 -D__GLIBCXX_TYPE_INT_N_0=__int128")
endif()

# Set clang flags for cross-compiling to IR.
# IR_COMPILE is #defined for the cross compile to remove code that bloats the IR.
# Note that we don't enable any optimization. We want unoptimized IR since we will be
# modifying it at runtime, then re-compiling (and optimizing) the modified code. The final
# optimizations will be less effective if the initial code is also optimized.
set(CLANG_IR_CXX_FLAGS "-std=gnu++11" "-c" "-emit-llvm" "-D__STDC_CONSTANT_MACROS" "-D__STDC_FORMAT_MACROS" "-D__STDC_LIMIT_MACROS" "-DIR_COMPILE" "-DNDEBUG" "-DHAVE_INTTYPES_H" "-DHAVE_NETINET_IN_H" "-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG" "-D__GLIBCXX_BITSIZE_INT_N_0=128" "-D__GLIBCXX_TYPE_INT_N_0=__int128" "-U_GLIBCXX_USE_FLOAT128")

# CMake really doesn't like adding link directories and wants absolute paths
# Reconstruct it with LLVM_MODULE_LIBS and LLVM_LIBRARY_DIR
string(REPLACE " " ";" LIBS_LIST ${LLVM_MODULE_LIBS})
set (LLVM_MODULE_LIBS "-ldl")
foreach (LIB ${LIBS_LIST})
  set(LLVM_MODULE_LIBS ${LLVM_MODULE_LIBS} "${LLVM_LIBRARY_DIR}/${LIB}")
endforeach(LIB)

message(STATUS "LLVM include dir: ${LLVM_INCLUDE_DIR}")
message(STATUS "LLVM lib dir: ${LLVM_LIBRARY_DIR}")
message(STATUS "LLVM libs: ${LLVM_MODULE_LIBS}")
message(STATUS "LLVM compile flags: ${LLVM_CFLAGS}")

# When the Toolchain is used we use LLVM 3.3 that was built in a different path that it
# is invoked from, and a GCC that resides in a different location. LVVM 3.3 relies on
# hard-coded path information about where to find the system headers and does not support
# specifying the -gcc-toolchain flag to dynamically provide this information. Because of
# these reasons we need to manually add the system c++ headers to the path when we
# compile the IR code with clang.

# Check the release version of the system to set the correct flags.
# You may have to modify the ${CLANG_BASE_FLAGS} by you own.
execute_process(COMMAND lsb_release -si OUTPUT_VARIABLE LINUX_VERSION)
string(TOLOWER ${LINUX_VERSION} LINUX_VERSION_LOWER)
message(STATUS "${LINUX_VERSION_LOWER}")

if(${LINUX_VERSION_LOWER} MATCHES "ubuntu")
    set(CLANG_BASE_FLAGS
        "-I/usr/include/c++/5/"
        "-I/usr/include/x86_64-linux-gnu/c++/5/")
elseif(${LINUX_VERSION_LOWER} MATCHES "centos")
    set(CLANG_BASE_FLAGS
        "-I/usr/include/c++/4.8.5/"
        "-I/usr/include/c++/4.8.5/x86_64-redhat-linux/")
elseif(${LINUX_VERSION_LOWER} MATCHES "fedora")
    set(CLANG_BASE_FLAGS
        "-I/usr/include/c++/7/"
        "-I/usr/include/c++/7/x86_64-redhat-linux/")
else()
    message(FATAL_ERROR "Currently not support system ${LINUX_VERSION}")
endif()

set(CLANG_INCLUDE_FLAGS
    "-I${BASE_DIR}/src"
    "-I${GENSRC_DIR}"
    "-I${THIRDPARTY_DIR}/include"
    "-I${THIRDPARTY_DIR}/include/thrift/"
    "-I${THIRDPARTY_DIR}/include/event/"
)

# Set include dirs
include_directories(${LLVM_INCLUDE_DIR})
include_directories( 
    ${SRC_DIR}/
    ${TEST_DIR}/
    ${GENSRC_DIR}/
    ${THIRDPARTY_DIR}/include/
    ${THIRDPARTY_DIR}/include/thrift/
    ${THIRDPARTY_DIR}/include/event/
)

# Set libraries
set(WL_START_GROUP "-Wl,--start-group")
set(WL_END_GROUP "-Wl,--end-group")

# Set Palo libraries
set (PALO_LINK_LIBS
    ${WL_START_GROUP}
    Agent
    CodeGen
    Common
    Exec
    Exprs
    Gutil
    Olap
    Runtime
    RPC
    Service
    Udf
    Util
    PaloGen
    Webserver
    TestUtil
    AES
    ${WL_END_GROUP}
)

# Set thirdparty libraries
set (PALO_LINK_LIBS ${PALO_LINK_LIBS}
    protobuf
    lzo
    snappy
    ${Boost_LIBRARIES}
    ${LLVM_MODULE_LIBS}
    # popt
    thrift
    thriftnb
    ${WL_START_GROUP}
    glog
    gflags
    re2
    pprof
    unwind
    tcmalloc
    unwind
    lz4
    libevent
    ${LIBZ}
    ${LIBBZ2}
    mysql
    curl
    ${WL_END_GROUP}
    -lrt
    -lbfd
    -liberty
    openssl
    crypto
    #-fsanitize=address
    #-lboost_date_time
)

# Set libraries for test
set (TEST_LINK_LIBS ${PALO_LINK_LIBS} gmock LLVMSupport)

# Set CXX flags
SET(CXX_COMMON_FLAGS "-msse4.2 -Wall -Wno-sign-compare -Wno-deprecated -pthread")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG -D__STDC_FORMAT_MACROS")

# Add by zhaochun: use gnu++11 for make_unsigned<__int128>
SET(CMAKE_CXX_FLAGS "-g -O2 -ggdb -Wno-unused-local-typedefs -Wno-strict-aliasing -std=gnu++11 -DPERFORMANCE -D_FILE_OFFSET_BITS=64")

# use address sanitizer, commented the malloc in ld flags
# SET(CMAKE_CXX_FLAGS "-g -ggdb -Wno-unused-local-typedefs -Wno-strict-aliasing -std=gnu++11 -DPERFORMANCE -fsanitize=address -fno-omit-frame-pointer -DADDRESS_SANITIZER")
SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")

MESSAGE(STATUS "Compiler Flags: ${CMAKE_CXX_FLAGS}")

# Thrift requires these two definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H)

# Only build static libs
set(BUILD_SHARED_LIBS OFF)

if (${MAKE_TEST} STREQUAL "ON")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage -lgcov")
    add_definitions(-DBE_TEST)
endif ()

add_subdirectory(${SRC_DIR}/codegen)
add_subdirectory(${SRC_DIR}/common)
add_subdirectory(${SRC_DIR}/util)
add_subdirectory(${SRC_DIR}/gen_cpp)
add_subdirectory(${SRC_DIR}/gutil)
add_subdirectory(${SRC_DIR}/olap)
add_subdirectory(${SRC_DIR}/agent)
add_subdirectory(${SRC_DIR}/http)
add_subdirectory(${SRC_DIR}/service)
add_subdirectory(${SRC_DIR}/exec)
add_subdirectory(${SRC_DIR}/exprs)
add_subdirectory(${SRC_DIR}/udf)
add_subdirectory(${SRC_DIR}/runtime)
add_subdirectory(${SRC_DIR}/testutil)
add_subdirectory(${SRC_DIR}/rpc)
add_subdirectory(${SRC_DIR}/aes)

if (${MAKE_TEST} STREQUAL "ON")
    add_subdirectory(${TEST_DIR}/agent)
    add_subdirectory(${TEST_DIR}/olap)
    add_subdirectory(${TEST_DIR}/common)
    add_subdirectory(${TEST_DIR}/util)
    add_subdirectory(${TEST_DIR}/udf)
    add_subdirectory(${TEST_DIR}/exec)
    add_subdirectory(${TEST_DIR}/exprs)
    add_subdirectory(${TEST_DIR}/runtime)
    add_subdirectory(${TEST_DIR}/udf)
endif ()

# Install be
install(DIRECTORY DESTINATION ${OUTPUT_DIR})
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/bin)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/conf)

install(FILES
    ${BASE_DIR}/../bin/start_be.sh
    ${BASE_DIR}/../bin/stop_be.sh
    DESTINATION ${OUTPUT_DIR}/bin)

install(FILES
    ${BASE_DIR}/../conf/be.conf
    DESTINATION ${OUTPUT_DIR}/conf)

# Utility CMake function to make specifying tests and benchmarks less verbose
FUNCTION(ADD_BE_TEST TEST_NAME)
    # This gets the directory where the test is from (e.g. 'exprs' or 'runtime')
    get_filename_component(DIR_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)
    get_filename_component(TEST_DIR_NAME ${TEST_NAME} PATH)
    get_filename_component(TEST_FILE_NAME ${TEST_NAME} NAME)

    ADD_EXECUTABLE(${TEST_FILE_NAME} ${TEST_NAME}.cpp)
    TARGET_LINK_LIBRARIES(${TEST_FILE_NAME} ${TEST_LINK_LIBS})
    SET_TARGET_PROPERTIES(${TEST_FILE_NAME} PROPERTIES COMPILE_FLAGS "-Dprivate=public -Dprotected=public")
    if (NOT "${TEST_DIR_NAME}" STREQUAL "")
        SET_TARGET_PROPERTIES(${TEST_FILE_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}/${DIR_NAME}/${TEST_DIR_NAME}")
    endif()
    ADD_TEST(${TEST_FILE_NAME} "${BUILD_OUTPUT_ROOT_DIRECTORY}/${DIR_NAME}/${TEST_NAME}")
ENDFUNCTION()

