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

# Unity build for the small common TUs. Seven files leak file-scope macros
# (SET_FIELD/UPDATE_FIELD, MEMORY_SANITIZER, DW_* tables, the DEFINE_*_METRIC
# families, USE_PHDR_CACHE/UNW_LOCAL_ONLY) and stay individual -- that set
# also keeps the three same-name metric-hook statics (_s_hook_name) apart.
set(COMMON_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/config.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/demangle.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dwarf.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/metrics/doris_metrics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/metrics/jvm_metrics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/metrics/system_metrics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/phdr_cache.cpp)
set_source_files_properties(${COMMON_UNITY_SKIP} PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
if (ENABLE_UNITY_BUILD)
    set_target_properties(Common PROPERTIES UNITY_BUILD ON UNITY_BUILD_BATCH_SIZE 0)
endif()

# Generate env_config.h according to env_config.h.in
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/env_config.h.in ${GENSRC_DIR}/common/env_config.h)
target_include_directories(Common PUBLIC ${GENSRC_DIR}/common/)
