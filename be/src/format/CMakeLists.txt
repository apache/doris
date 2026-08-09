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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/format")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/format")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp)
file(GLOB_RECURSE FORMAT_V2_SRC_FILES CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/../format_v2/*.cpp)
list(APPEND SRC_FILES ${FORMAT_V2_SRC_FILES})

add_library(Format STATIC ${SRC_FILES})

pch_reuse(Format)

# Unity build scoped to format_v2/: the v2 readers are the maintained line and
# share one reader/expr header closure. The format/ (v1) tree is slated for
# removal in a future version, so it is not worth the v1/v2 twin-symbol
# deduplication a joint unity would require: every v1 file stays individual
# until the tree is deleted. Within v2, adbc_reader.cpp stays individual
# because its file-scope RETURN_IF_ADBC_ERROR macro must not leak into unity
# siblings. Batch 8 bounds jumbo-TU size and memory.
set(FORMAT_UNITY_SKIP ${SRC_FILES})
list(FILTER FORMAT_UNITY_SKIP EXCLUDE REGEX ".*/format_v2/.*")
list(APPEND FORMAT_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/../format_v2/table/adbc_reader.cpp)
set_source_files_properties(${FORMAT_UNITY_SKIP} PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
if (ENABLE_UNITY_BUILD)
    set_target_properties(Format PROPERTIES UNITY_BUILD ON UNITY_BUILD_BATCH_SIZE 8)
endif()
