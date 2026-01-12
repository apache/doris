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

add_subdirectory(cmake-protect)

# Use all .cpp files in this directory as sources
file(GLOB VECTOR_LIB_SRC CONFIGURE_DEPENDS *.cpp)

find_package(OpenMP REQUIRED)

add_library(ann_index STATIC ${VECTOR_LIB_SRC})
target_link_libraries(ann_index PUBLIC faiss OpenMP::OpenMP_CXX)

# Some header files from faiss are used by doris, they will break compile check.
target_compile_options(ann_index PRIVATE -Wno-shadow-field)
