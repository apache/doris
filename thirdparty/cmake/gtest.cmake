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

# googletest — hand-written add_library (no add_subdirectory)
set(GTEST_SRC_DIR ${TP_SOURCE_DIR}/googletest-release-1.12.1/googletest)
set(GMOCK_SRC_DIR ${TP_SOURCE_DIR}/googletest-release-1.12.1/googlemock)

# gtest — uses gtest-all.cc which aggregates all source files
add_library(gtest STATIC
    ${GTEST_SRC_DIR}/src/gtest-all.cc
)

target_include_directories(gtest SYSTEM PUBLIC
    ${GTEST_SRC_DIR}/include
)

target_include_directories(gtest PRIVATE
    ${GTEST_SRC_DIR}
)

target_compile_options(gtest PRIVATE -fPIC -w)

set_target_properties(gtest PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# gtest_main
add_library(gtest_main STATIC
    ${GTEST_SRC_DIR}/src/gtest_main.cc
)

target_link_libraries(gtest_main PUBLIC gtest)
target_compile_options(gtest_main PRIVATE -fPIC -w)

set_target_properties(gtest_main PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# gmock
add_library(gmock STATIC
    ${GMOCK_SRC_DIR}/src/gmock-all.cc
)

target_include_directories(gmock SYSTEM PUBLIC
    ${GMOCK_SRC_DIR}/include
)

target_include_directories(gmock PRIVATE
    ${GMOCK_SRC_DIR}
)

target_link_libraries(gmock PUBLIC gtest)
target_compile_options(gmock PRIVATE -fPIC -w)

set_target_properties(gmock PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# gmock_main
add_library(gmock_main STATIC
    ${GMOCK_SRC_DIR}/src/gmock_main.cc
)

target_link_libraries(gmock_main PUBLIC gmock)
target_compile_options(gmock_main PRIVATE -fPIC -w)

set_target_properties(gmock_main PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
