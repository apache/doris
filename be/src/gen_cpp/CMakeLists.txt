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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/gen_cpp")

set (GEN_CPP_DIR ${GENSRC_DIR}/gen_cpp)
file(GLOB SRC_FILES CONFIGURE_DEPENDS
    ${GEN_CPP_DIR}/*.cpp
    ${GEN_CPP_DIR}/*.cc
)
list(FILTER SRC_FILES EXCLUDE REGEX "_server\\.skeleton\\.cpp$")

add_compile_options(-Wno-return-type)

# keep everything in one library, the object files reference
# each other
add_library(DorisGen STATIC ${SRC_FILES})

# Unity build for the thrift-generated .cpp half (pre-scanned clean: no
# file-scope macros or duplicate statics). Generated sources cannot be edited,
# so anything that clashes can only be opted out, never fixed:
# - every protobuf .cc stays individual: protoc emits the same file-scope
#   statics (schemas[], file_default_instances[]) in each one, a structural
#   clash for any merged pair
# - the two heaviest thrift TUs stay individual so no batch inherits them
set(GEN_UNITY_SKIP ${SRC_FILES})
list(FILTER GEN_UNITY_SKIP INCLUDE REGEX "\\.cc$")
list(APPEND GEN_UNITY_SKIP
    ${GEN_CPP_DIR}/FrontendService_types.cpp
    ${GEN_CPP_DIR}/PlanNodes_types.cpp)
set_source_files_properties(${GEN_UNITY_SKIP} PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
set_target_properties(DorisGen PROPERTIES UNITY_BUILD ${DORIS_UNITY_BUILD}
                                          UNITY_BUILD_BATCH_SIZE 8)

if (COMPILER_CLANG)
    target_compile_options(DorisGen PRIVATE
    -Wno-implicit-int-conversion
    -Wno-shorten-64-to-32
    )
endif()

# Setting these files as code-generated lets make clean and incremental builds work
# correctly
# set_source_files_properties(${SRC_FILES} PROPERTIES GENERATED TRUE)

#add_dependencies(DorisGen thrift-cpp)
#add_dependencies(Opcode function)
