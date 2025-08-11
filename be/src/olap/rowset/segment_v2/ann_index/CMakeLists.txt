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

set(VECTOR_LIB_SRC
    ann_index_files.h
    ann_index_iterator.h
    ann_index_iterator.cpp
    ann_index_metric.h
    ann_index_metric.cpp
    ann_index_reader.h
    ann_index_reader.cpp
    ann_index_writer.h
    ann_index_writer.cpp
    ann_range_search_runtime.cpp
    ann_range_search_runtime.h
    ann_search_params.h
    ann_topn_runtime.cpp
    ann_topn_runtime.h
    faiss_vector_index.h
    faiss_vector_index.cpp
    
    vector_index.h
)

find_package(OpenMP REQUIRED)

add_library(ann_index STATIC ${VECTOR_LIB_SRC})
target_link_libraries(ann_index PUBLIC faiss OpenMP::OpenMP_CXX)

# Some header files from faiss are used by doris, they will break compile check.
target_compile_options(ann_index PRIVATE -Wno-shadow-field)
