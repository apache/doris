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

# curl — hand-written add_library (no add_subdirectory)
set(CURL_SRC_DIR ${TP_SOURCE_DIR}/curl-8.2.1)
set(CURL_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/curl_gen)
file(MAKE_DIRECTORY ${CURL_GEN_DIR}/lib)

# Pre-generated config header (Linux x86_64, gcc/clang)
file(COPY "${TP_SOURCE_DIR}/_pregenerated/curl/curl_config.h" DESTINATION ${CURL_GEN_DIR}/lib)

# ---------- add_library ----------
file(GLOB CURL_LIB_SRCS "${CURL_SRC_DIR}/lib/*.c")
file(GLOB CURL_VAUTH_SRCS "${CURL_SRC_DIR}/lib/vauth/*.c")
file(GLOB CURL_VTLS_SRCS "${CURL_SRC_DIR}/lib/vtls/*.c")
file(GLOB CURL_VSSH_SRCS "${CURL_SRC_DIR}/lib/vssh/*.c")
file(GLOB CURL_VQUIC_SRCS "${CURL_SRC_DIR}/lib/vquic/*.c")

add_library(libcurl STATIC
    ${CURL_LIB_SRCS}
    ${CURL_VAUTH_SRCS}
    ${CURL_VTLS_SRCS}
    ${CURL_VSSH_SRCS}
    ${CURL_VQUIC_SRCS}
)

target_include_directories(libcurl SYSTEM PUBLIC
    ${CURL_SRC_DIR}/include
)

target_include_directories(libcurl PRIVATE
    ${CURL_SRC_DIR}/lib
    ${CURL_GEN_DIR}/lib
    ${CMAKE_CURRENT_BINARY_DIR}/openssl/include
)

target_compile_options(libcurl PRIVATE -fPIC -w)
target_compile_definitions(libcurl PRIVATE
    HAVE_CONFIG_H
    BUILDING_LIBCURL
    CURL_STATICLIB
)
target_compile_definitions(libcurl PUBLIC CURL_STATICLIB)

find_package(Threads REQUIRED)
target_link_libraries(libcurl PRIVATE Threads::Threads)

set_target_properties(libcurl PROPERTIES
    OUTPUT_NAME curl
    POSITION_INDEPENDENT_CODE ON
)

add_library(curl ALIAS libcurl)
if(NOT TARGET CURL::libcurl)
    add_library(CURL::libcurl INTERFACE IMPORTED GLOBAL)
    target_link_libraries(CURL::libcurl INTERFACE libcurl)
endif()

set(CURL_FOUND TRUE CACHE BOOL "" FORCE)
set(CURL_INCLUDE_DIR "${CURL_SRC_DIR}/include" CACHE PATH "" FORCE)
set(CURL_INCLUDE_DIRS "${CURL_SRC_DIR}/include" CACHE PATH "" FORCE)
set(CURL_LIBRARY libcurl CACHE STRING "" FORCE)
set(CURL_LIBRARIES libcurl CACHE STRING "" FORCE)
