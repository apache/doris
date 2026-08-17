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

# Unity build for the whole Storage target: segment/rowset/tablet/compaction/
# index glue TUs re-parse the same storage header closure once per file, so
# batching them cuts the repeated frontend work. Batch 12 bounds jumbo-TU size
# and memory. Opted out:
# - seven files whose file-scope macros (CL_MAX_PATH and friends,
#   IS_CHINESE_CHAR, APPLY_FOR_PRIMITITYPE, RETURN_IF_NOT_EOF_AND_OK,
#   FORMAT_*_ADD_JSON_NODE, RETURN_IF_ERROR_) must not leak into unity siblings
# - the three heaviest template-instantiation TUs (predicate creators) which
#   would dominate any batch they join
# - segment/variant/v2/variant_storage_cell.cpp: it and variant_assembler.cpp
#   define the same anonymous-namespace helper, which conflicts in one unity batch
set(STORAGE_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/index/inverted/inverted_index_compound_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index/inverted/inverted_index_fs_directory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index/inverted/tokenizer/basic/basic_tokenizer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index/zone_map/zone_map_index.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/iterator/vcollect_iterator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tablet/tablet.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task/engine_clone_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/predicate/predicate_creator_comparison.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/predicate/predicate_creator_in_list_in.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/predicate/predicate_creator_in_list_not_in.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index/inverted/similarity/collection_statistics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/segment/variant/v2/variant_storage_cell.cpp)
if (ENABLE_VARIANT_NESTED_GROUP)
    # Out-of-tree module sources swapped into this target: unity hygiene
    # unaudited, keep them individual.
    list(APPEND STORAGE_UNITY_SKIP ${VARIANT_NESTED_GROUP_SOURCES})
endif()
doris_skip_unity_inclusion(${STORAGE_UNITY_SKIP})
set_target_properties(Storage PROPERTIES UNITY_BUILD ${DORIS_UNITY_BUILD}
                                         UNITY_BUILD_BATCH_SIZE 12)
