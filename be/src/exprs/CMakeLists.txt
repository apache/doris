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
target_link_libraries(Exprs PRIVATE ann_index)

pch_reuse(Exprs)

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