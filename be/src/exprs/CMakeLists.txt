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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exprs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exprs")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp *.cc)

# Add generated flex/bison sources for geo WKT parsing
set(SRC_FILES ${SRC_FILES}
    ${GENSRC_DIR}/geo/wkt_lex.l.cpp
    ${GENSRC_DIR}/geo/wkt_yacc.y.cpp
)

add_library(Exprs STATIC ${SRC_FILES})
# function_array_distance uses faiss headers (platform_macros.h, distances.h),
# which are exported by ann_index via PUBLIC linkage with faiss.
target_link_libraries(Exprs
    PRIVATE ann_index
    PUBLIC DataSketches::HLL)

pch_reuse(Exprs)

# Unity build for the Exprs glue: ~250 registration/wrapper TUs share the
# vexpr/factory header closure. Batch 8 bounds jumbo-TU size and memory.
# Opted out and kept as individual TUs:
#  - the flex/bison outputs and tldLookup.generated.cpp (generated tables and
#    macros would clash),
#  - files whose file-scope macros must not leak into unity siblings
#    (fmod_fast, function_bitmap_variadic, function_date_or_datetime_computation,
#    function_datetime_floor_ceil, function_string_misc, json_functions,
#    math_functions),
#  - the heavy template-instantiation TUs (>15s wall or >2.2GB RSS in the
#    compile bench: the min_max/collect/topn/percentile aggregate family,
#    in.cpp, multiply.cpp, round.cpp, function_array_aggregation.cpp, ...);
#    their codegen is per-file and would only stack into jumbo poles,
#  - files that tests compile a second time by #including the .cpp
#    (function_variant_element, uuid, function_jsonb_transform): the test
#    object must shadow a never-pulled archive member, but a unity batch is
#    pulled in for its siblings and the linker sees a duplicate definition.
set(EXPRS_UNITY_SKIP
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_array_agg.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_avg.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_bit.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_collect_limit.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_collect_no_limit.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_distinct.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_group_array_intersect.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_group_array_union.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_histogram.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_map_combinator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_min_max_any.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_min_max_by.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_min_max_max.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_min_max_min.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_percentile.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_sum.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_topn_array.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_topn_weighted.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate/aggregate_function_uniq.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/ai/ai_functions.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/array/function_array_aggregation.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/cast/function_cast_decimal.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/fmod_fast.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/function_bitmap_variadic.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/function_date_or_datetime_computation.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/function_datetime_floor_ceil.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/function_jsonb_transform.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/function_string_misc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/function_variant_element.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/in.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/minus.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/multiply.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/plus.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/round.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/url/tldLookup.generated.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/function/uuid.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/json_functions.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/math_functions.cpp
    ${GENSRC_DIR}/geo/wkt_lex.l.cpp
    ${GENSRC_DIR}/geo/wkt_yacc.y.cpp)
doris_skip_unity_inclusion(${EXPRS_UNITY_SKIP})
set_target_properties(Exprs PROPERTIES UNITY_BUILD ${DORIS_UNITY_BUILD}
                                       UNITY_BUILD_BATCH_SIZE 8)

# Flex/Bison generation rules for geo WKT parsing
add_custom_command(
    OUTPUT ${GENSRC_DIR}/geo/wkt_lex.l.cpp ${GENSRC_DIR}/geo/wkt_lex.l.h
    COMMAND mkdir -p ${GENSRC_DIR}/geo
    COMMAND flex --header-file=${GENSRC_DIR}/geo/wkt_lex.l.h --outfile=${GENSRC_DIR}/geo/wkt_lex.l.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/function/geo/wkt_lex.l
    MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/function/geo/wkt_lex.l)
set_source_files_properties(${GENSRC_DIR}/geo/wkt_lex.l.cpp PROPERTIES GENERATED TRUE)

add_custom_command(
    OUTPUT ${GENSRC_DIR}/geo/wkt_yacc.y.cpp  ${GENSRC_DIR}/geo/wkt_yacc.y.hpp
    COMMAND mkdir -p ${GENSRC_DIR}/geo
    COMMAND bison --output=${GENSRC_DIR}/geo/wkt_yacc.y.cpp ${CMAKE_CURRENT_SOURCE_DIR}/function/geo/wkt_yacc.y
    MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/function/geo/wkt_yacc.y)
set_source_files_properties(${GENSRC_DIR}/geo/wkt_yacc.y.cpp PROPERTIES GENERATED TRUE)

include(CheckCXXCompilerFlag)
set(WARNING_OPTION "-Wno-unused-but-set-variable")
check_cxx_compiler_flag(${WARNING_OPTION} HAS_WARNING_OPTION)
if (HAS_WARNING_OPTION)
    target_compile_options(Exprs PRIVATE ${WARNING_OPTION} "-Wno-unused-macros")
endif()