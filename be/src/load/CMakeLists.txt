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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/load")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/load")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp)
add_library(Load STATIC ${SRC_FILES})

pch_reuse(Load)

# Unity build: load channel/delta-writer/group-commit glue sharing one header
# closure. Two files leak file-scope macros (UNKNOWN_ID_FOR_TEST,
# HANDLE_ERROR/HANDLE_MULTI_TABLE_ERROR) and stay individual.
set(LOAD_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/channel/load_stream.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/routine_load/routine_load_task_executor.cpp)
set_source_files_properties(${LOAD_UNITY_SKIP} PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
if (ENABLE_UNITY_BUILD)
    set_target_properties(Load PROPERTIES UNITY_BUILD ON UNITY_BUILD_BATCH_SIZE 12)
endif()
