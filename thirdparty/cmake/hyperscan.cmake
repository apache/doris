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

# hyperscan — hand-written add_library (no add_subdirectory)
set(_HS_SRC "${TP_SOURCE_DIR}/hyperscan-5.4.2")
set(_HS_GEN "${CMAKE_CURRENT_BINARY_DIR}/hyperscan")
file(MAKE_DIRECTORY ${_HS_GEN})
file(MAKE_DIRECTORY ${_HS_GEN}/src/parser)

# ============================================================================
# Pre-generated config headers and ragel output (Linux x86_64)
# ============================================================================
file(COPY "${TP_SOURCE_DIR}/_pregenerated/hyperscan/config.h" DESTINATION ${_HS_GEN})
file(COPY "${TP_SOURCE_DIR}/_pregenerated/hyperscan/hs_version.h" DESTINATION ${_HS_GEN})
file(COPY "${TP_SOURCE_DIR}/_pregenerated/hyperscan/Parser.cpp" DESTINATION ${_HS_GEN}/src/parser)
file(COPY "${TP_SOURCE_DIR}/_pregenerated/hyperscan/control_verbs.cpp" DESTINATION ${_HS_GEN}/src/parser)

# ============================================================================
# Source files — GLOB all .c/.cpp from src/, plus ragel-generated files
# hyperscan has 231 source files, all under src/ (no tests in src/)
# ============================================================================
file(GLOB_RECURSE _HS_SRCS
    "${_HS_SRC}/src/*.c"
    "${_HS_SRC}/src/*.cpp"
)

# Remove dispatcher.c (fat runtime only, FAT_RUNTIME=OFF)
list(FILTER _HS_SRCS EXCLUDE REGEX "dispatcher\\.c$")
# Remove noodle_engine_avx512.c (requires AVX-512 support, BUILD_AVX512=OFF)
list(FILTER _HS_SRCS EXCLUDE REGEX "noodle_engine_avx512\\.c$")
# Remove noodle_engine_sse.c and noodle_engine_avx2.c (#include'd by noodle_engine.c, not standalone)
list(FILTER _HS_SRCS EXCLUDE REGEX "noodle_engine_sse\\.c$")
list(FILTER _HS_SRCS EXCLUDE REGEX "noodle_engine_avx2\\.c$")
# Remove all dump files (require DUMP_SUPPORT which is off in release)
list(FILTER _HS_SRCS EXCLUDE REGEX "dump")

# Add ragel-generated files
list(APPEND _HS_SRCS
    ${_HS_GEN}/src/parser/Parser.cpp
    ${_HS_GEN}/src/parser/control_verbs.cpp
)

# ============================================================================
# Library target
# ============================================================================
add_library(hs STATIC ${_HS_SRCS})

target_include_directories(hs PUBLIC
    ${_HS_GEN}             # config.h, hs_version.h
)

target_include_directories(hs PRIVATE
    ${_HS_SRC}/src         # main source
    ${_HS_GEN}/src         # generated parser
    ${_HS_SRC}             # for src/ relative includes
)

# Boost headers (used by nfagraph for graph algorithms)
# boost-patched is in hyperscan's own include/ directory
target_include_directories(hs SYSTEM PRIVATE
    ${TP_SOURCE_DIR}/boost_1_81_0
    ${_HS_SRC}/include
)

target_compile_definitions(hs PRIVATE
    HAVE_SSE2
    HAVE_SSE41
    HAVE_SSSE3
)

target_compile_options(hs PRIVATE -mssse3 -msse4.1 -msse4.2)

# AVX2 flags for specific files
set_source_files_properties(
    ${_HS_SRC}/src/fdr/teddy_avx2.c
    ${_HS_SRC}/src/util/masked_move.c
    ${_HS_SRC}/src/hwlm/noodle_engine_avx2.c
    PROPERTIES COMPILE_FLAGS "-mavx2"
)

set_target_properties(hs PROPERTIES
    OUTPUT_NAME hs
    POSITION_INDEPENDENT_CODE ON
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
)

# Create include structure for "hs/hs.h"
set(HS_INC_DIR ${CMAKE_CURRENT_BINARY_DIR}/hyperscan_headers/include)
file(GLOB HS_SRC_HEADERS "${_HS_SRC}/src/hs*.h")
file(COPY ${HS_SRC_HEADERS} DESTINATION "${HS_INC_DIR}/hs")
# Also copy generated hs_version.h
file(COPY "${_HS_GEN}/hs_version.h" DESTINATION "${HS_INC_DIR}/hs")

target_include_directories(hs INTERFACE ${HS_INC_DIR})
add_library(hyperscan ALIAS hs)
