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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/core")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/core")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp)
add_library(Core STATIC ${SRC_FILES})

pch_reuse(Core)
# wide_integer_from_double.cpp defines DORIS_WIDE_INTEGER_FROM_DOUBLE_IMPL_TU
# before including wide_integer_impl.h to compile the from-double bodies exactly
# once. The PCH already contains that header (via storage/olap_common.h) in
# declaration-only mode, and its pre-spent include guard would leave the explicit
# instantiations below without definitions. Opt this single TU out of the PCH.
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/wide_integer_from_double.cpp
                            PROPERTIES SKIP_PRECOMPILE_HEADERS ON)

# Unity build: column/data_type/serde glue TUs share the core header closure.
# Opt-outs:
# - column/column_vector.cpp: the heaviest template-instantiation TU in the
#   target (~40s alone); it would dominate any batch it joins
# - three macro-leak files (WRITE_INTEGRAL_COLUMN_TO_ORC,
#   DECLARE_DECIMAL_COMPARISON, DELARE_DATE_ADD_INTERVAL)
# - wide_integer_from_double.cpp: macro-leak, and it is the PCH-skipped
#   explicit-instantiation TU above -- it must keep its own compile
# - data_type_serde.cpp: holds member-level explicit instantiations of serde
#   members whose class-level explicit instantiations live in the per-type
#   serde TUs; merging both forms into one TU is a duplicate explicit
#   instantiation
# - the value/variant/ family (9s of slot time in total): copied file-local
#   helpers with diverging signatures (require_bytes, write_unsigned, ...) --
#   not worth renaming for the gain
set(CORE_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/column/column_vector.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/data_type_serde/data_type_number_serde.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/data_type_serde/data_type_serde.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/field.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/value/vdatetime_value.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/value/variant/variant_batch_builder.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/value/variant/variant_canonical.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/value/variant/variant_field.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/value/variant/variant_metadata.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/value/variant/variant_scalar.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/value/variant/variant_value.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/wide_integer_from_double.cpp)
set_source_files_properties(${CORE_UNITY_SKIP} PROPERTIES SKIP_UNITY_BUILD_INCLUSION ON)
if (ENABLE_UNITY_BUILD)
    set_target_properties(Core PROPERTIES UNITY_BUILD ON UNITY_BUILD_BATCH_SIZE 12)
endif()
