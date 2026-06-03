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

# gflags — hand-written add_library (no add_subdirectory)
set(GFLAGS_SRC_DIR ${TP_SOURCE_DIR}/gflags-2.2.2)

# ---------- Template variable definitions (Linux) ----------
# Match gflags 2.2 CMakeLists default: primary namespace "google", secondary
# alias "gflags" (generated via gflags_ns.h.in -> gflags_gflags.h below).
# BE call sites use both `google::` and `gflags::` (e.g. be/test/testutil/
# run_all_tests.cpp:109), so the alias is required.
set(GFLAGS_NAMESPACE "google")
set(GFLAGS_IS_A_DLL 0)
set(GFLAGS_ATTRIBUTE_UNUSED " __attribute((unused))")

# gflags_declare.h.in variables
set(HAVE_STDINT_H 1)
set(HAVE_SYS_TYPES_H 1)
set(HAVE_INTTYPES_H 1)
set(GFLAGS_INTTYPES_FORMAT_C99 1)
set(GFLAGS_INTTYPES_FORMAT_BSD 0)
set(GFLAGS_INTTYPES_FORMAT_VC7 0)

# gflags.h.in: include the secondary-namespace alias header (gflags_gflags.h).
set(INCLUDE_GFLAGS_NS_H "// Import gflags library symbols into alternative/deprecated namespace(s)\n#include \"gflags_gflags.h\"")

# defines.h.in variables (Linux)
set(HAVE_SYS_STAT_H 1)
set(HAVE_UNISTD_H 1)
set(HAVE_FNMATCH_H 1)
set(HAVE_STRTOLL 1)
set(HAVE_PTHREAD 1)
set(HAVE_RWLOCK 1)

# ---------- configure_file: public headers ----------
set(GFLAGS_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/gflags/include/gflags)
file(MAKE_DIRECTORY ${GFLAGS_GEN_DIR})

configure_file(
    "${GFLAGS_SRC_DIR}/src/gflags_declare.h.in"
    "${GFLAGS_GEN_DIR}/gflags_declare.h"
    @ONLY
)

configure_file(
    "${GFLAGS_SRC_DIR}/src/gflags.h.in"
    "${GFLAGS_GEN_DIR}/gflags.h"
    @ONLY
)

configure_file(
    "${GFLAGS_SRC_DIR}/src/gflags_completions.h.in"
    "${GFLAGS_GEN_DIR}/gflags_completions.h"
    @ONLY
)

# Secondary-namespace alias header: imports symbols from `google::` into `gflags::`.
# gflags_ns.h.in uses @ns@ (lowercase) and @NS@ (uppercase).
set(ns "gflags")
set(NS "GFLAGS")
configure_file(
    "${GFLAGS_SRC_DIR}/src/gflags_ns.h.in"
    "${GFLAGS_GEN_DIR}/gflags_gflags.h"
    @ONLY
)
unset(ns)
unset(NS)

# ---------- configure_file: internal defines.h ----------
set(GFLAGS_INTERNAL_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/gflags/internal)
file(MAKE_DIRECTORY ${GFLAGS_INTERNAL_GEN_DIR})

configure_file(
    "${GFLAGS_SRC_DIR}/src/defines.h.in"
    "${GFLAGS_INTERNAL_GEN_DIR}/defines.h"
)

# ---------- add_library ----------
add_library(gflags_static STATIC
    ${GFLAGS_SRC_DIR}/src/gflags.cc
    ${GFLAGS_SRC_DIR}/src/gflags_reporting.cc
    ${GFLAGS_SRC_DIR}/src/gflags_completions.cc
)

# PUBLIC: consumers include <gflags/gflags.h>
target_include_directories(gflags_static SYSTEM PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}/gflags/include
)

# PRIVATE: source files include "config.h", "mutex.h", "util.h" from src/,
# "defines.h" from internal/, and "gflags/gflags.h" via PUBLIC include.
target_include_directories(gflags_static PRIVATE
    ${GFLAGS_SRC_DIR}/src
    ${GFLAGS_INTERNAL_GEN_DIR}
)

target_compile_options(gflags_static PRIVATE -fPIC -w)
target_compile_definitions(gflags_static PUBLIC GFLAGS_IS_A_DLL=0)

set_target_properties(gflags_static PROPERTIES
    OUTPUT_NAME gflags
    POSITION_INDEPENDENT_CODE ON
)

# Link pthreads (multi-threaded build)
find_package(Threads REQUIRED)
target_link_libraries(gflags_static PRIVATE Threads::Threads)

# COMMON_THIRDPARTY uses "gflags"
add_library(gflags ALIAS gflags_static)
