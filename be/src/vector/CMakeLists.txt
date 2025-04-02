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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/vector")

set(VECTOR_LIB_SRC
        vector_index.h
        stream_wrapper.h
)

set(VECTOR_LIB_DEPENDENCIES)

if (BUILD_FAISS)
    # append faiss src to VECTOR_LIB_SRC
    list(APPEND VECTOR_LIB_SRC
        faiss_vector_index.h
        faiss_vector_index.cpp
    )
    list(APPEND VECTOR_LIB_DEPENDENCIES faiss)
endif()

add_library(vector OBJECT ${VECTOR_LIB_SRC})
target_link_libraries(vector PRIVATE ${VECTOR_LIB_DEPENDENCIES})

# Make the headers available to targets that link against the vector library
target_include_directories(vector INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)

# If you need to install these headers
# install(FILES ${HEADER_FILES} DESTINATION include/vector)

