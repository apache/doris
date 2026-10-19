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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/cloud")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp)
add_library(Cloud STATIC ${SRC_FILES})

pch_reuse(Cloud)

# Unity build: homogeneous cloud glue TUs sharing the CloudStorageEngine /
# meta-service RPC header closure. Batch 12 bounds jumbo-TU size and memory.
# The two http action TUs stay individual: sibling TUs' file-scope
# `using namespace ErrorCode` makes the unscoped HttpStatus enum constants
# (OK, NOT_FOUND, ...) shadow ErrorCode variables under -Wshadow -Werror.
set(CLOUD_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/cloud_compaction_action.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/injection_point_action.cpp)
doris_skip_unity_inclusion(${CLOUD_UNITY_SKIP})
set_target_properties(Cloud PROPERTIES UNITY_BUILD ${DORIS_UNITY_BUILD}
                                       UNITY_BUILD_BATCH_SIZE 12)
