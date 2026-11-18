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

include(ExternalProject)
# Re-extracted sources must be newer than objects from an older archive.
if (POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

set(JEMALLOC_VERSION 5.3.1)
set(JEMALLOC_SHA256 3826bc80232f22ed5c4662f3034f799ca316e819103bdc7bb99018a421706f92)
set(DORIS_DEPENDENCY_DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../.dependency-downloads"
    CACHE PATH "Directory for verified dependency source archives (may be prepopulated offline)")
set(JEMALLOC_BUILD_JOBS 8 CACHE STRING "Parallel jobs for the jemalloc Makefile build")
find_program(JEMALLOC_MAKE_EXECUTABLE NAMES gmake make REQUIRED)
find_program(JEMALLOC_PATCH_EXECUTABLE NAMES patch REQUIRED)

# A changed patch gets a fresh source tree; never apply it twice to an existing tree.
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/jemalloc_hook.patch")
file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/jemalloc_hook.patch" patch_hash)
set(jemalloc_prefix "${CMAKE_CURRENT_BINARY_DIR}/${JEMALLOC_VERSION}-${patch_hash}")
set(jemalloc_install "${jemalloc_prefix}/installed")
set(jemalloc_include "${jemalloc_install}/include")
set(jemalloc_library "${jemalloc_install}/lib/libjemalloc_doris.a")
file(MAKE_DIRECTORY "${jemalloc_include}")

# Autoconf runs outside CMake's compiler rules. Pass the selected compiler and its
# launcher explicitly, without inheriting BE's C++/sanitizer flags.
set(jemalloc_cc "${CMAKE_C_COMPILER_LAUNCHER};${CMAKE_C_COMPILER}")
list(FILTER jemalloc_cc EXCLUDE REGEX "^$")
list(JOIN jemalloc_cc " " jemalloc_cc)
set(jemalloc_cflags "-O3 -fno-omit-frame-pointer -fPIC -g ${CMAKE_C_COMPILER_ARG1}")
if (CMAKE_SYSROOT)
    string(APPEND jemalloc_cflags " --sysroot=${CMAKE_SYSROOT}")
endif()
if (CMAKE_C_COMPILER_TARGET)
    string(APPEND jemalloc_cflags " --target=${CMAKE_C_COMPILER_TARGET}")
endif()
set(jemalloc_options
    --with-install-suffix=_doris --with-jemalloc-prefix=je --enable-prof
    --disable-cxx --disable-libdl --disable-shared)
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64|AARCH64)")
    list(APPEND jemalloc_options --with-lg-page=16)
endif()
set(jemalloc_cppflags "")
set(jemalloc_ldflags "")
set(jemalloc_libs "")
set(jemalloc_verify_command "${CMAKE_COMMAND}" -E true)
if (NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # Require Doris's GNU libunwind, not a host library or libgcc fallback.
    get_target_property(jemalloc_unwind_library libunwind IMPORTED_LOCATION)
    get_filename_component(jemalloc_unwind_dir "${jemalloc_unwind_library}" DIRECTORY)
    set(jemalloc_cppflags "-I${THIRDPARTY_DIR}/include")
    set(jemalloc_ldflags "-L${jemalloc_unwind_dir} -L${THIRDPARTY_DIR}/lib")
    set(jemalloc_libs "-llzma -lz")
    list(APPEND jemalloc_options --enable-prof-libunwind --disable-prof-libgcc)
    set(jemalloc_verify_command "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_SOURCE_DIR}/verify-profiler.cmake")
endif()

ExternalProject_Add(jemalloc_ep
    URL "https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_VERSION}/jemalloc-${JEMALLOC_VERSION}.tar.bz2"
    URL_HASH "SHA256=${JEMALLOC_SHA256}"
    DOWNLOAD_DIR "${DORIS_DEPENDENCY_DOWNLOAD_DIR}"
    PREFIX "${jemalloc_prefix}"
    PATCH_COMMAND "${JEMALLOC_PATCH_EXECUTABLE}" -p0 -i "${CMAKE_CURRENT_SOURCE_DIR}/jemalloc_hook.patch"
    CONFIGURE_COMMAND "${CMAKE_COMMAND}" -E env
        "CC=${jemalloc_cc}" "CFLAGS=${jemalloc_cflags}"
        "CPPFLAGS=${jemalloc_cppflags}" "LDFLAGS=${jemalloc_ldflags}" "LIBS=${jemalloc_libs}"
        <SOURCE_DIR>/configure "--prefix=${jemalloc_install}" ${jemalloc_options}
        COMMAND ${jemalloc_verify_command}
    BUILD_COMMAND "${JEMALLOC_MAKE_EXECUTABLE}" "-j${JEMALLOC_BUILD_JOBS}"
    INSTALL_COMMAND "${JEMALLOC_MAKE_EXECUTABLE}" install_lib_static install_include
        COMMAND "${CMAKE_COMMAND}" -E rename
            "${jemalloc_include}/jemalloc/jemalloc_doris.h"
            "${jemalloc_include}/jemalloc/jemalloc.h"
    BUILD_BYPRODUCTS "${jemalloc_library}" "${jemalloc_include}/jemalloc/jemalloc.h")

# A verifier change must also invalidate an already completed configure step.
ExternalProject_Add_StepDependencies(jemalloc_ep configure
    "${CMAKE_CURRENT_SOURCE_DIR}/verify-profiler.cmake")

add_library(jemalloc STATIC IMPORTED GLOBAL)
set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION "${jemalloc_library}"
    INTERFACE_INCLUDE_DIRECTORIES "${jemalloc_include}")
add_dependencies(jemalloc jemalloc_ep)

# BE static libraries include allocator headers before the final executable links
# jemalloc. Carry the generation dependency without linking the allocator into tools.
add_library(jemalloc_headers INTERFACE IMPORTED GLOBAL)
target_include_directories(jemalloc_headers SYSTEM INTERFACE "${jemalloc_include}")
add_dependencies(jemalloc_headers jemalloc_ep)
set(JEMALLOC_INCLUDE_DIR "${jemalloc_include}" PARENT_SCOPE)
