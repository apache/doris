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

# CMake Initial Cache for paimon-cpp
# Configures paimon-cpp to reuse selected Doris third-party libraries
# Usage: cmake -C paimon-cpp-cache.cmake ...

# Get the Doris thirdparty installation directory from environment
set(DORIS_THIRDPARTY_DIR "$ENV{TP_INSTALL_DIR}" CACHE PATH "Doris thirdparty install directory")

if(NOT DORIS_THIRDPARTY_DIR)
    message(FATAL_ERROR "TP_INSTALL_DIR environment variable must be set")
endif()

message(STATUS "Using Doris thirdparty libraries from: ${DORIS_THIRDPARTY_DIR}")

# Set CMAKE_PREFIX_PATH to help find_package locate our libraries
set(CMAKE_PREFIX_PATH "${DORIS_THIRDPARTY_DIR};${CMAKE_PREFIX_PATH}" CACHE STRING "Search path for find_package")

# Library and include paths
set(DORIS_LIB_DIR "${DORIS_THIRDPARTY_DIR}/lib" CACHE PATH "Doris library directory")
set(DORIS_INCLUDE_DIR "${DORIS_THIRDPARTY_DIR}/include" CACHE PATH "Doris include directory")

# ============================================================================
# ZLIB - Reuse from Doris (version 1.3.1)
# ============================================================================
set(ZLIB_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "ZLIB root directory")
set(ZLIB_LIBRARY "${DORIS_LIB_DIR}/libz.a" CACHE FILEPATH "ZLIB library")
set(ZLIB_INCLUDE_DIR "${DORIS_INCLUDE_DIR}" CACHE PATH "ZLIB include directory")

# ============================================================================
# ZSTD - Reuse from Doris (version 1.5.7)
# ============================================================================
set(ZSTD_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "ZSTD root directory")
set(ZSTD_LIBRARY "${DORIS_LIB_DIR}/libzstd.a" CACHE FILEPATH "ZSTD library")
set(ZSTD_INCLUDE_DIR "${DORIS_INCLUDE_DIR}" CACHE PATH "ZSTD include directory")

# ============================================================================
# LZ4 - Reuse from Doris (version 1.9.4)
# ============================================================================
set(LZ4_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "LZ4 root directory")
set(LZ4_LIBRARY "${DORIS_LIB_DIR}/liblz4.a" CACHE FILEPATH "LZ4 library")
set(LZ4_INCLUDE_DIR "${DORIS_INCLUDE_DIR}" CACHE PATH "LZ4 include directory")

# ============================================================================
# glog - Reuse from Doris (version 0.6.0)
# Note: Paimon-cpp uses 0.7.1, but 0.6.0 is compatible
# ============================================================================
set(GLOG_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "glog root directory")
set(GLOG_LIBRARY "${DORIS_LIB_DIR}/libglog.a" CACHE FILEPATH "glog library")
set(GLOG_INCLUDE_DIR "${DORIS_INCLUDE_DIR}" CACHE PATH "glog include directory")

# ============================================================================
# Arrow, Protobuf, Thrift - NOT reusing from Doris
# paimon-cpp will build its own versions with symbol visibility=hidden
# to prevent conflicts with Doris's versions
# ============================================================================

# ============================================================================
# Snappy - Reuse from Doris
# ============================================================================
set(Snappy_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Snappy root directory")
set(SNAPPY_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Snappy root directory (legacy)")
set(SNAPPY_LIBRARY "${DORIS_LIB_DIR}/libsnappy.a" CACHE FILEPATH "Snappy library")
set(SNAPPY_INCLUDE_DIR "${DORIS_INCLUDE_DIR}" CACHE PATH "Snappy include directory")

# ============================================================================
# Build configuration
# ============================================================================
set(CMAKE_POSITION_INDEPENDENT_CODE ON CACHE BOOL "Build with -fPIC")
set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type")

# Symbol visibility control to prevent conflicts with Doris
# paimon-cpp builds Arrow/ORC/etc with hidden symbols to avoid conflicts
set(CMAKE_CXX_VISIBILITY_PRESET "hidden" CACHE STRING "Hide C++ symbols by default")
set(CMAKE_C_VISIBILITY_PRESET "hidden" CACHE STRING "Hide C symbols by default")
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON CACHE BOOL "Hide inline function symbols")

# Verify that required libraries exist
if(NOT EXISTS "${ZLIB_LIBRARY}")
    message(FATAL_ERROR "ZLIB library not found: ${ZLIB_LIBRARY}")
endif()
if(NOT EXISTS "${ZSTD_LIBRARY}")
    message(FATAL_ERROR "ZSTD library not found: ${ZSTD_LIBRARY}")
endif()
if(NOT EXISTS "${LZ4_LIBRARY}")
    message(FATAL_ERROR "LZ4 library not found: ${LZ4_LIBRARY}")
endif()
if(NOT EXISTS "${SNAPPY_LIBRARY}")
    message(FATAL_ERROR "Snappy library not found: ${SNAPPY_LIBRARY}")
endif()
if(NOT EXISTS "${GLOG_LIBRARY}")
    message(FATAL_ERROR "glog library not found: ${GLOG_LIBRARY}")
endif()

message(STATUS "========================================")
message(STATUS "Paimon-cpp Library Reuse Configuration")
message(STATUS "========================================")
message(STATUS "Reusing from Doris:")
message(STATUS "  ✓ ZLIB, ZSTD, LZ4, Snappy, glog")
message(STATUS "")
message(STATUS "Building separately (symbol visibility=hidden):")
message(STATUS "  - Arrow, Protobuf, Thrift, ORC")
message(STATUS "  - RapidJSON, TBB")
message(STATUS "========================================")
