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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/gen_cpp")

set (GEN_CPP_DIR ${GENSRC_DIR}/gen_cpp)
set(SRC_FILES
    ${GEN_CPP_DIR}/cloud.pb.cc
    ${GEN_CPP_DIR}/olap_file.pb.cc
    ${GEN_CPP_DIR}/types.pb.cc
    ${GEN_CPP_DIR}/segment_v2.pb.cc
    ${GEN_CPP_DIR}/olap_common.pb.cc
)

# keep everything in one library, the object files reference
# each other
add_library(CloudGen STATIC ${SRC_FILES})

# Setting these files as code-generated lets make clean and incremental builds
# work correctly
set_source_files_properties(${SRC_FILES} PROPERTIES GENERATED TRUE)
