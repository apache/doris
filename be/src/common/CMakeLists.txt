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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/common")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp)
add_library(Common STATIC ${SRC_FILES})

pch_reuse(Common)

# asan_repro_uninstrumented.cpp emulates an un-instrumented third-party
# library (like libthrift) for the ASAN stale-stack-poison reproducer, so it
# must be compiled without sanitizer instrumentation, without builtin
# memset/memcpy expansion (the calls must reach the ASAN interceptors), and
# without the sanitizer-flavored precompiled header.
# See asan-glog-rca.md at the repository root.
set_source_files_properties(asan_repro_uninstrumented.cpp PROPERTIES
    COMPILE_OPTIONS "-fno-sanitize=all;-fno-builtin"
    SKIP_PRECOMPILE_HEADERS ON)

# Generate env_config.h according to env_config.h.in
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/env_config.h.in ${GENSRC_DIR}/common/env_config.h)
target_include_directories(Common PUBLIC ${GENSRC_DIR}/common/)
