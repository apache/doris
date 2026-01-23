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
# Arrow - EXPERIMENTAL: Trying to reuse from Doris (version 17.0.0)
#
# WARNING: Build configurations differ significantly:
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
# Potential risks:
#   - Linker errors if paimon needs DATASET/COMPUTE but Doris doesn't have them
#   - Runtime issues if Doris's extra features (Flight) cause ABI mismatches
#
# If this causes problems, comment out these lines to let paimon build its own Arrow
# ============================================================================
set(Arrow_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Arrow root directory")
set(Parquet_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Parquet root directory")
set(ARROW_HOME "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Arrow home directory")
set(Parquet_HOME "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Parquet home directory")

# Arrow libraries - paimon-cpp needs all of these
set(ARROW_LIBRARY "${DORIS_LIB_DIR}/libarrow.a" CACHE FILEPATH "Arrow library")
set(ARROW_STATIC_LIB "${DORIS_LIB_DIR}/libarrow.a" CACHE FILEPATH "Arrow static library")
set(PARQUET_LIBRARY "${DORIS_LIB_DIR}/libparquet.a" CACHE FILEPATH "Parquet library")
set(PARQUET_STATIC_LIB "${DORIS_LIB_DIR}/libparquet.a" CACHE FILEPATH "Parquet static library")

# Required for paimon-cpp (uses Arrow Dataset API)
set(ARROW_DATASET_LIBRARY "${DORIS_LIB_DIR}/libarrow_dataset.a" CACHE FILEPATH "Arrow dataset library")
set(ARROW_DATASET_STATIC_LIB "${DORIS_LIB_DIR}/libarrow_dataset.a" CACHE FILEPATH "Arrow dataset static library")
set(ARROW_ACERO_LIBRARY "${DORIS_LIB_DIR}/libarrow_acero.a" CACHE FILEPATH "Arrow acero library")
set(ARROW_ACERO_STATIC_LIB "${DORIS_LIB_DIR}/libarrow_acero.a" CACHE FILEPATH "Arrow acero static library")

# ============================================================================
# Protobuf - Reuse from Doris (required by Doris's Arrow with Flight/gRPC)
# When reusing Arrow from Doris, we must also provide protobuf to avoid
# paimon-cpp downloading and building its own version
# ============================================================================
set(Protobuf_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Protobuf root directory")
set(PROTOBUF_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Protobuf root directory (legacy)")
set(Protobuf_INCLUDE_DIR "${DORIS_INCLUDE_DIR}" CACHE PATH "Protobuf include directory")
set(Protobuf_LIBRARY "${DORIS_LIB_DIR}/libprotobuf.a" CACHE FILEPATH "Protobuf library")
set(Protobuf_LITE_LIBRARY "${DORIS_LIB_DIR}/libprotobuf-lite.a" CACHE FILEPATH "Protobuf lite library")
set(Protobuf_PROTOC_LIBRARY "${DORIS_LIB_DIR}/libprotoc.a" CACHE FILEPATH "Protobuf compiler library")
set(Protobuf_PROTOC_EXECUTABLE "${DORIS_THIRDPARTY_DIR}/bin/protoc" CACHE FILEPATH "Protobuf compiler")
# Legacy variables
set(PROTOBUF_LIBRARY "${DORIS_LIB_DIR}/libprotobuf.a" CACHE FILEPATH "Protobuf library (legacy)")
set(PROTOBUF_INCLUDE_DIR "${DORIS_INCLUDE_DIR}" CACHE PATH "Protobuf include directory (legacy)")
set(PROTOBUF_PROTOC_EXECUTABLE "${DORIS_THIRDPARTY_DIR}/bin/protoc" CACHE FILEPATH "Protobuf compiler (legacy)")
# Tell paimon/Arrow to use system protobuf
set(Protobuf_SOURCE "SYSTEM" CACHE STRING "Use system protobuf")
set(PROTOBUF_SOURCE "SYSTEM" CACHE STRING "Use system protobuf (legacy)")

# ============================================================================
# Thrift - Reuse from Doris (required by Doris's Arrow with Flight)
# When reusing Arrow from Doris, we must also provide thrift to avoid
# paimon-cpp downloading and building its own version
# ============================================================================
set(Thrift_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Thrift root directory")
set(THRIFT_ROOT "${DORIS_THIRDPARTY_DIR}" CACHE PATH "Thrift root directory (legacy)")
set(Thrift_INCLUDE_DIR "${DORIS_INCLUDE_DIR}" CACHE PATH "Thrift include directory")
set(Thrift_LIBRARY "${DORIS_LIB_DIR}/libthrift.a" CACHE FILEPATH "Thrift library")
set(Thrift_STATIC_LIB "${DORIS_LIB_DIR}/libthrift.a" CACHE FILEPATH "Thrift static library")
# Legacy variables
set(THRIFT_LIBRARY "${DORIS_LIB_DIR}/libthrift.a" CACHE FILEPATH "Thrift library (legacy)")
set(THRIFT_INCLUDE_DIR "${DORIS_INCLUDE_DIR}" CACHE PATH "Thrift include directory (legacy)")
set(THRIFT_STATIC_LIB "${DORIS_LIB_DIR}/libthrift.a" CACHE FILEPATH "Thrift static library (legacy)")
# Tell Arrow to use system thrift
set(Thrift_SOURCE "SYSTEM" CACHE STRING "Use system thrift")
set(THRIFT_SOURCE "SYSTEM" CACHE STRING "Use system thrift (legacy)")

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

# CRITICAL: Symbol visibility control to prevent ORC symbol conflicts
# When PAIMON_ENABLE_ORC=ON, paimon-cpp builds its own Apache ORC library
# Doris also has Apache ORC in be/src/apache-orc/
# Setting visibility to hidden prevents paimon's ORC symbols from conflicting with Doris's ORC
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
if(NOT EXISTS "${ARROW_LIBRARY}")
    message(FATAL_ERROR "Arrow library not found: ${ARROW_LIBRARY}\nPlease rebuild Arrow: ./build-thirdparty.sh arrow")
endif()
if(NOT EXISTS "${PARQUET_LIBRARY}")
    message(FATAL_ERROR "Parquet library not found: ${PARQUET_LIBRARY}\nPlease rebuild Arrow: ./build-thirdparty.sh arrow")
endif()
if(NOT EXISTS "${ARROW_DATASET_LIBRARY}")
    message(FATAL_ERROR "Arrow Dataset library not found: ${ARROW_DATASET_LIBRARY}\nThis means Doris's Arrow was not built with ARROW_DATASET=ON.\nPlease rebuild Arrow: ./build-thirdparty.sh arrow")
endif()
if(NOT EXISTS "${ARROW_ACERO_LIBRARY}")
    message(FATAL_ERROR "Arrow Acero library not found: ${ARROW_ACERO_LIBRARY}\nThis means Doris's Arrow was not built with ARROW_COMPUTE=ON.\nPlease rebuild Arrow: ./build-thirdparty.sh arrow")
endif()
if(NOT EXISTS "${PROTOBUF_LIBRARY}")
    message(FATAL_ERROR "Protobuf library not found: ${PROTOBUF_LIBRARY}\nPlease rebuild: ./build-thirdparty.sh protobuf")
endif()
if(NOT EXISTS "${THRIFT_LIBRARY}")
    message(FATAL_ERROR "Thrift library not found: ${THRIFT_LIBRARY}\nPlease rebuild: ./build-thirdparty.sh thrift")
endif()
if(NOT EXISTS "${PROTOBUF_PROTOC_EXECUTABLE}")
    message(FATAL_ERROR "Protobuf compiler (protoc) not found: ${PROTOBUF_PROTOC_EXECUTABLE}\nPlease rebuild: ./build-thirdparty.sh protobuf")
endif()

message(STATUS "")
message(STATUS "========================================")
message(STATUS "Paimon-cpp Library Reuse Configuration")
message(STATUS "========================================")
message(STATUS "")
message(STATUS "Reusing from Doris:")
message(STATUS "  ✓ ZLIB: ${ZLIB_LIBRARY}")
message(STATUS "  ✓ ZSTD: ${ZSTD_LIBRARY}")
message(STATUS "  ✓ LZ4: ${LZ4_LIBRARY}")
message(STATUS "  ✓ Snappy: ${SNAPPY_LIBRARY}")
message(STATUS "  ✓ fmt: ${FMT_LIBRARY}")
message(STATUS "  ✓ Protobuf: ${PROTOBUF_LIBRARY}")
message(STATUS "  ✓ Thrift: ${THRIFT_LIBRARY}")
message(STATUS "  ⚠ Arrow: ${ARROW_LIBRARY}")
message(STATUS "    ├─ Dataset: ${ARROW_DATASET_LIBRARY}")
message(STATUS "    ├─ Acero: ${ARROW_ACERO_LIBRARY}")
message(STATUS "    └─ Parquet: ${PARQUET_LIBRARY}")
message(STATUS "")
message(STATUS "  NOTE: Arrow reuse is EXPERIMENTAL")
message(STATUS "  Doris Arrow has DATASET+COMPUTE+FLIGHT enabled")
message(STATUS "  This requires Protobuf and Thrift (now reused from Doris)")
message(STATUS "  Watch for potential ABI issues or runtime crashes")
message(STATUS "")
message(STATUS "Building separately:")
message(STATUS "  - TBB (not used by Doris)")
message(STATUS "  - RapidJSON (header-only, TLS conflicts prevent sharing)")
message(STATUS "  - glog (to avoid conflicts)")
message(STATUS "  - Apache ORC (with symbol visibility hidden to avoid conflicts)")
message(STATUS "")
message(STATUS "ORC Symbol Isolation:")
message(STATUS "  - paimon-cpp builds its own Apache ORC with -fvisibility=hidden")
message(STATUS "  - Doris has Apache ORC in be/src/apache-orc/")
message(STATUS "  - Both can coexist due to symbol visibility isolation")
message(STATUS "")
message(STATUS "If you encounter issues, edit paimon-cpp-cache.cmake")
message(STATUS "and comment out Arrow configuration to build separately")
message(STATUS "========================================")
