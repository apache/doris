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

# c-ares — hand-written add_library (no add_subdirectory)
set(CARES_SRC_DIR ${TP_SOURCE_DIR}/c-ares-1.19.1)
set(CARES_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/cares)
file(MAKE_DIRECTORY ${CARES_GEN_DIR})

# Pre-generated config headers (Linux x86_64, gcc/clang)
file(COPY "${TP_SOURCE_DIR}/_pregenerated/cares/ares_config.h" DESTINATION ${CARES_GEN_DIR})
file(COPY "${TP_SOURCE_DIR}/_pregenerated/cares/ares_build.h" DESTINATION ${CARES_GEN_DIR})

# ---------- add_library ----------
file(GLOB CARES_SRCS "${CARES_SRC_DIR}/src/lib/*.c")
list(FILTER CARES_SRCS EXCLUDE REGEX "windows_port\\.c$")

add_library(c-ares_static STATIC ${CARES_SRCS})

target_include_directories(c-ares_static SYSTEM PUBLIC
    ${CARES_SRC_DIR}/include
    ${CARES_GEN_DIR}
)

target_include_directories(c-ares_static PRIVATE
    ${CARES_SRC_DIR}/src/lib
    ${CARES_GEN_DIR}
)

target_compile_options(c-ares_static PRIVATE -fPIC -w)
target_compile_definitions(c-ares_static PUBLIC CARES_STATICLIB)
target_compile_definitions(c-ares_static PRIVATE HAVE_CONFIG_H)

set_target_properties(c-ares_static PROPERTIES
    OUTPUT_NAME cares
    POSITION_INDEPENDENT_CODE ON
)

add_library(cares ALIAS c-ares_static)
if(NOT TARGET c-ares::cares)
    add_library(c-ares::cares INTERFACE IMPORTED GLOBAL)
    target_link_libraries(c-ares::cares INTERFACE c-ares_static)
endif()
if(NOT TARGET c-ares::cares_static)
    add_library(c-ares::cares_static INTERFACE IMPORTED GLOBAL)
    target_link_libraries(c-ares::cares_static INTERFACE c-ares_static)
endif()
set(c-ares_FOUND TRUE CACHE BOOL "" FORCE)
