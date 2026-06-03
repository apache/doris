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

# snappy — hand-written add_library (no add_subdirectory)
set(SNAPPY_SRC_DIR ${TP_SOURCE_DIR}/snappy-1.1.10)

# ---------- configure_file: config.h ----------
# Values hardcoded for Linux x86_64 (gcc/clang).
set(HAVE_ATTRIBUTE_ALWAYS_INLINE 1)
set(HAVE_BUILTIN_CTZ 1)
set(HAVE_BUILTIN_EXPECT 1)
set(HAVE_FUNC_MMAP 1)
set(HAVE_FUNC_SYSCONF 1)
set(HAVE_LIBLZO2 0)
set(HAVE_LIBZ 0)
set(HAVE_LIBLZ4 0)
set(HAVE_SYS_MMAN_H 1)
set(HAVE_SYS_RESOURCE_H 1)
set(HAVE_SYS_TIME_H 1)
set(HAVE_SYS_UIO_H 1)
set(HAVE_UNISTD_H 1)
set(HAVE_WINDOWS_H 0)
set(SNAPPY_HAVE_SSSE3 0)
set(SNAPPY_HAVE_X86_CRC32 0)
set(SNAPPY_HAVE_BMI2 0)
set(SNAPPY_HAVE_NEON 0)
set(SNAPPY_HAVE_NEON_CRC32 0)
set(SNAPPY_IS_BIG_ENDIAN 0)

configure_file(
    "${SNAPPY_SRC_DIR}/cmake/config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/snappy/config.h"
    @ONLY
)

# ---------- configure_file: snappy-stubs-public.h ----------
# This template uses ${} substitution (not @ONLY).
set(HAVE_SYS_UIO_H_01 1)
set(PROJECT_VERSION_MAJOR 1)
set(PROJECT_VERSION_MINOR 1)
set(PROJECT_VERSION_PATCH 10)

configure_file(
    "${SNAPPY_SRC_DIR}/snappy-stubs-public.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/snappy/snappy-stubs-public.h"
)

# ---------- add_library ----------
add_library(snappy STATIC
    ${SNAPPY_SRC_DIR}/snappy.cc
    ${SNAPPY_SRC_DIR}/snappy-c.cc
    ${SNAPPY_SRC_DIR}/snappy-sinksource.cc
    ${SNAPPY_SRC_DIR}/snappy-stubs-internal.cc
)

target_include_directories(snappy SYSTEM PUBLIC
    ${SNAPPY_SRC_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/snappy
)

# -frtti: Doris inherits snappy classes (SnappySlicesSource)
# -DHAVE_CONFIG_H: snappy sources conditionally include config.h
target_compile_options(snappy PRIVATE -fPIC -w -frtti)
target_compile_definitions(snappy PRIVATE HAVE_CONFIG_H)

set_target_properties(snappy PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# ---------- nested headers (snappy/snappy.h) ----------
set(SNAPPY_NESTED_DIR ${CMAKE_CURRENT_BINARY_DIR}/snappy_headers/include/snappy)
file(MAKE_DIRECTORY ${SNAPPY_NESTED_DIR})
file(GLOB _SNAPPY_HEADERS "${SNAPPY_SRC_DIR}/snappy*.h")
file(COPY ${_SNAPPY_HEADERS} DESTINATION ${SNAPPY_NESTED_DIR})
file(COPY "${CMAKE_CURRENT_BINARY_DIR}/snappy/snappy-stubs-public.h"
     DESTINATION ${SNAPPY_NESTED_DIR})
add_library(snappy_headers INTERFACE)
target_include_directories(snappy_headers SYSTEM INTERFACE
    ${CMAKE_CURRENT_BINARY_DIR}/snappy_headers/include
)

# ---------- IMPORTED targets for Arrow/Parquet compatibility ----------
# Use INTERFACE IMPORTED (not STATIC IMPORTED) so the build system can track
# the snappy target's output file. STATIC IMPORTED with IMPORTED_LOCATION
# creates a file-level dependency that ninja cannot connect to the snappy target.
if(NOT TARGET Snappy::snappy)
    add_library(Snappy::snappy INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Snappy::snappy INTERFACE snappy)
endif()

if(NOT TARGET Snappy::snappy-static)
    add_library(Snappy::snappy-static INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Snappy::snappy-static INTERFACE snappy)
endif()

# Cache variables for Arrow's find_package(Snappy)
set(SNAPPY_LIBRARY snappy CACHE STRING "" FORCE)
set(SNAPPY_INCLUDE_DIR "${SNAPPY_SRC_DIR}" CACHE STRING "" FORCE)
set(Snappy_FOUND TRUE CACHE BOOL "" FORCE)
set(SNAPPY_FOUND TRUE CACHE BOOL "" FORCE)
set(SnappyAlt_FOUND TRUE CACHE BOOL "" FORCE)
set(Snappy_TARGET "Snappy::snappy-static" CACHE STRING "" FORCE)
