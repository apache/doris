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
# This file configures paimon-cpp to reuse Doris's pre-built third-party libraries
# instead of building them from source.
#
# Usage: cmake -C paimon-cpp-cache.cmake ...

# Disable building third-party dependencies that we provide from Doris
# This prevents paimon-cpp from downloading and building these libraries

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
# Arrow - NOT reusing from Doris (version 17.0.0)
# Although versions match, build configurations differ significantly:
#
# Doris Arrow config:         Paimon-cpp Arrow config:
#   ARROW_FLIGHT=ON              (not set - OFF)
#   ARROW_FLIGHT_SQL=ON          (not set - OFF)
#   ARROW_WITH_GRPC=ON           (not set - OFF)
#   ARROW_WITH_PROTOBUF=ON       (not set - OFF)
#   ARROW_ORC=OFF (fixed)        ARROW_ORC=OFF
#   ARROW_DATASET=(maybe OFF)    ARROW_DATASET=ON
#   ARROW_COMPUTE=(maybe OFF)    ARROW_COMPUTE=ON
#   ARROW_SIMD_LEVEL=default     ARROW_SIMD_LEVEL=NONE
#
# These differences cause ABI incompatibility:
#   - Different vtable layouts (Flight adds virtual methods)
#   - Different symbol exports (GRPC/Protobuf dependencies)
#   - Different template instantiations (SIMD levels)
#
# Attempting to reuse would cause:
#   - Linker errors (undefined symbols)
#   - Runtime crashes (vtable mismatches)
#   - Memory corruption (struct layout differences)
#
# Cost of building Arrow separately: ~10 minutes (acceptable)
# Risk of reusing: Potential crashes and data corruption (unacceptable)
# ============================================================================
# set(Arrow_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Arrow root directory")
# set(Parquet_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Parquet root directory")
# set(ARROW_LIBRARY "${DORIS_LIB_DIR}/libarrow.a" CACHE FILEPATH "Arrow library")
# set(PARQUET_LIBRARY "${DORIS_LIB_DIR}/libparquet.a" CACHE FILEPATH "Parquet library")

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

message(STATUS "Paimon-cpp will use the following Doris libraries:")
message(STATUS "  - ZLIB: ${ZLIB_LIBRARY}")
message(STATUS "  - ZSTD: ${ZSTD_LIBRARY}")
message(STATUS "  - LZ4: ${LZ4_LIBRARY}")
message(STATUS "  - Snappy: ${SNAPPY_LIBRARY}")
message(STATUS "  - fmt: ${FMT_LIBRARY}")
message(STATUS "")
message(STATUS "Paimon-cpp will build its own:")
message(STATUS "  - TBB (not used by Doris)")
message(STATUS "  - RapidJSON (header-only, cannot be shared due to TLS conflicts)")
message(STATUS "  - Arrow (different build configurations)")
