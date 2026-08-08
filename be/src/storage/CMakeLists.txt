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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/storage")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/storage")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp)

# Files in the ann_index directory use faiss header files
# Some of Doris's compilation check options fail on these header files, so exclude
# files in the ann_index directory. They are compiled separately as a .a library
# and linked by Storage.
list(FILTER SRC_FILES EXCLUDE REGEX ".*/storage/index/ann/.*\\.cpp$")

if (ENABLE_VARIANT_NESTED_GROUP)
    list(REMOVE_ITEM SRC_FILES
            "${CMAKE_CURRENT_SOURCE_DIR}/segment/variant/nested_group_provider.cpp")
    file(GLOB_RECURSE VARIANT_NESTED_GROUP_SOURCES CONFIGURE_DEPENDS
            "${CMAKE_CURRENT_SOURCE_DIR}/../${VARIANT_NESTED_GROUP_MODULE_DIR}/*.cpp")
    list(APPEND SRC_FILES ${VARIANT_NESTED_GROUP_SOURCES})
endif()

add_library(Storage STATIC ${SRC_FILES})
target_link_libraries(Storage PRIVATE ann_index)

if (OS_MACOSX)
    target_compile_options(Storage PRIVATE -Wno-unused-lambda-capture)
endif()

pch_reuse(Storage)

# Unity build scoped to storage/index/: 115 homogeneous reader/writer/iterator
# TUs sharing one (CLucene-heavy) header closure. The rest of Storage keeps
# compiling individually. Batch 32 bounds jumbo-TU size and memory. Four index
# files stay individual because their file-scope macros (CL_MAX_PATH and
# friends, IS_CHINESE_CHAR, APPLY_FOR_PRIMITITYPE) must not leak into unity
# siblings.
set(STORAGE_UNITY_SKIP ${SRC_FILES})
list(FILTER STORAGE_UNITY_SKIP EXCLUDE REGEX ".*/storage/index/.*")
list(APPEND STORAGE_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/index/inverted/inverted_index_compound_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index/inverted/inverted_index_fs_directory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index/inverted/tokenizer/basic/basic_tokenizer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index/zone_map/zone_map_index.cpp)
set_source_files_properties(${STORAGE_UNITY_SKIP} PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
if (ENABLE_UNITY_BUILD)
    set_target_properties(Storage PROPERTIES UNITY_BUILD ON UNITY_BUILD_BATCH_SIZE 32)
endif()
