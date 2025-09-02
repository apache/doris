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

add_compile_options(-Wno-return-type)

# keep everything in one library, the object files reference 
# each other
add_library(DorisGen STATIC ${SRC_FILES})

if(CMAKE_CXX_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(CloudGen PRIVATE -Wno-error)
    
    target_compile_options(CloudGen PRIVATE
        -Wno-unused-variable
        -Wno-sign-compare
        -Wno-unused-function
        -Wno-deprecated-declarations
        -Wno-unused-parameter
        -Wno-missing-field-initializers
    )
endif()

# Setting these files as code-generated lets make clean and incremental builds work
# correctly
# set_source_files_properties(${SRC_FILES} PROPERTIES GENERATED TRUE)

#add_dependencies(DorisGen thrift-cpp)
#add_dependencies(Opcode function)

