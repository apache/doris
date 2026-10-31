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

# Read the pinned SDK timezone rules without changing SDK decoding or public interfaces.
target_include_directories(Format PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../contrib/apache-orc/c++/src
    ${BUILD_DIR}/apache-orc/c++/src)

pch_reuse(Format)

# Shared format utilities and writers remain individual translation units. Reader implementations
# under format_v2/ share one reader/expr header closure and use bounded unity batches. Keep the
# ADBC source individual so its file-scope status macro cannot leak into sibling sources.
set(FORMAT_UNITY_SKIP ${SRC_FILES})
list(FILTER FORMAT_UNITY_SKIP EXCLUDE REGEX ".*/format_v2/.*")
list(APPEND FORMAT_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/../format_v2/table/adbc_reader.cpp)
doris_skip_unity_inclusion(${FORMAT_UNITY_SKIP})
set_target_properties(Format PROPERTIES UNITY_BUILD ${DORIS_UNITY_BUILD}
                                        UNITY_BUILD_BATCH_SIZE 8)
