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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/information_schema")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/information_schema")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp)
add_library(InformationSchema STATIC ${SRC_FILES})

pch_reuse(InformationSchema)

# Unity build: these schema scanners are homogeneous glue code whose per-TU
# cost is dominated by re-parsing the same header closure once per file.
# Batch size 0 merges all sources into a single unity TU (~10x faster than
# compiling them individually). A new file whose file-scope symbols clash
# inside the unity TU can opt out via SKIP_UNITY_BUILD_INCLUSION.
set_target_properties(InformationSchema PROPERTIES UNITY_BUILD ${DORIS_UNITY_BUILD}
                                                   UNITY_BUILD_BATCH_SIZE 0)
