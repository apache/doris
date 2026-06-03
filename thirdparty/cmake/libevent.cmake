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

# libevent — hand-written add_library (no add_subdirectory)
set(LIBEVENT_SRC_DIR ${TP_SOURCE_DIR}/doris-thirdparty-libevent-2.1.12.1)
set(LIBEVENT_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/libevent/include)
file(MAKE_DIRECTORY ${LIBEVENT_GEN_DIR}/event2)

# Pre-generated config headers (Linux x86_64)
file(COPY "${TP_SOURCE_DIR}/_pregenerated/libevent/event-config.h"
     DESTINATION ${LIBEVENT_GEN_DIR}/event2)
file(COPY "${TP_SOURCE_DIR}/_pregenerated/libevent/evconfig-private.h"
     DESTINATION ${LIBEVENT_GEN_DIR})

# ---------- Source file lists (Linux/POSIX) ----------
set(LIBEVENT_CORE_SRCS
    ${LIBEVENT_SRC_DIR}/buffer.c
    ${LIBEVENT_SRC_DIR}/bufferevent.c
    ${LIBEVENT_SRC_DIR}/bufferevent_filter.c
    ${LIBEVENT_SRC_DIR}/bufferevent_pair.c
    ${LIBEVENT_SRC_DIR}/bufferevent_ratelim.c
    ${LIBEVENT_SRC_DIR}/bufferevent_sock.c
    ${LIBEVENT_SRC_DIR}/event.c
    ${LIBEVENT_SRC_DIR}/evmap.c
    ${LIBEVENT_SRC_DIR}/evthread.c
    ${LIBEVENT_SRC_DIR}/evutil.c
    ${LIBEVENT_SRC_DIR}/evutil_rand.c
    ${LIBEVENT_SRC_DIR}/evutil_time.c
    ${LIBEVENT_SRC_DIR}/listener.c
    ${LIBEVENT_SRC_DIR}/log.c
    ${LIBEVENT_SRC_DIR}/signal.c
    ${LIBEVENT_SRC_DIR}/strlcpy.c
    # Linux platform sources
    ${LIBEVENT_SRC_DIR}/epoll.c
    ${LIBEVENT_SRC_DIR}/poll.c
    ${LIBEVENT_SRC_DIR}/select.c
)

set(LIBEVENT_EXTRA_SRCS
    ${LIBEVENT_SRC_DIR}/event_tagging.c
    ${LIBEVENT_SRC_DIR}/http.c
    ${LIBEVENT_SRC_DIR}/evdns.c
    ${LIBEVENT_SRC_DIR}/evrpc.c
)

# Common include dirs
set(LIBEVENT_INCLUDE_DIRS
    ${LIBEVENT_SRC_DIR}/include
    ${LIBEVENT_SRC_DIR}/compat
    ${LIBEVENT_SRC_DIR}
    ${LIBEVENT_GEN_DIR}
)

# ---------- event_core_static ----------
add_library(event_core_static STATIC ${LIBEVENT_CORE_SRCS})
target_include_directories(event_core_static SYSTEM PUBLIC
    ${LIBEVENT_SRC_DIR}/include
    ${LIBEVENT_GEN_DIR}
)
target_include_directories(event_core_static PRIVATE ${LIBEVENT_INCLUDE_DIRS})
target_compile_options(event_core_static PRIVATE -fPIC -w)
target_compile_definitions(event_core_static PRIVATE HAVE_CONFIG_H)
set_target_properties(event_core_static PROPERTIES
    OUTPUT_NAME event_core
    POSITION_INDEPENDENT_CODE ON
)

# ---------- event_extra_static ----------
add_library(event_extra_static STATIC ${LIBEVENT_EXTRA_SRCS})
target_include_directories(event_extra_static PRIVATE ${LIBEVENT_INCLUDE_DIRS})
target_link_libraries(event_extra_static PUBLIC event_core_static)
target_compile_options(event_extra_static PRIVATE -fPIC -w)
target_compile_definitions(event_extra_static PRIVATE HAVE_CONFIG_H)
set_target_properties(event_extra_static PROPERTIES
    OUTPUT_NAME event_extra
    POSITION_INDEPENDENT_CODE ON
)

# ---------- event_static (core + extra combined) ----------
add_library(event_static STATIC ${LIBEVENT_CORE_SRCS} ${LIBEVENT_EXTRA_SRCS})
target_include_directories(event_static SYSTEM PUBLIC
    ${LIBEVENT_SRC_DIR}/include
    ${LIBEVENT_GEN_DIR}
)
target_include_directories(event_static PRIVATE ${LIBEVENT_INCLUDE_DIRS})
target_compile_options(event_static PRIVATE -fPIC -w)
target_compile_definitions(event_static PRIVATE HAVE_CONFIG_H)
set_target_properties(event_static PROPERTIES
    OUTPUT_NAME event
    POSITION_INDEPENDENT_CODE ON
)

# ---------- event_pthreads_static ----------
add_library(event_pthreads_static STATIC ${LIBEVENT_SRC_DIR}/evthread_pthread.c)
target_include_directories(event_pthreads_static PRIVATE ${LIBEVENT_INCLUDE_DIRS})
target_link_libraries(event_pthreads_static PUBLIC event_core_static)
target_compile_options(event_pthreads_static PRIVATE -fPIC -w)
target_compile_definitions(event_pthreads_static PRIVATE HAVE_CONFIG_H)
find_package(Threads REQUIRED)
target_link_libraries(event_pthreads_static PRIVATE Threads::Threads)
set_target_properties(event_pthreads_static PROPERTIES
    OUTPUT_NAME event_pthreads
    POSITION_INDEPENDENT_CODE ON
)

# ---------- event_openssl_static ----------
add_library(event_openssl_static STATIC ${LIBEVENT_SRC_DIR}/bufferevent_openssl.c)
target_include_directories(event_openssl_static PRIVATE
    ${LIBEVENT_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}/openssl/include
)
target_link_libraries(event_openssl_static PUBLIC event_core_static)
target_compile_options(event_openssl_static PRIVATE -fPIC -w)
target_compile_definitions(event_openssl_static PRIVATE HAVE_CONFIG_H)
set_target_properties(event_openssl_static PROPERTIES
    OUTPUT_NAME event_openssl
    POSITION_INDEPENDENT_CODE ON
)

# ---------- Aliases matching COMMON_THIRDPARTY ----------
add_library(libevent_core ALIAS event_core_static)
add_library(libevent ALIAS event_static)
add_library(libevent_pthreads ALIAS event_pthreads_static)
add_library(libevent_openssl ALIAS event_openssl_static)

# Libevent found vars for thrift
set(LIBEVENT_FOUND TRUE CACHE BOOL "" FORCE)
set(Libevent_FOUND TRUE CACHE BOOL "" FORCE)
set(LIBEVENT_LIBRARIES "event_core_static;event_extra_static" CACHE STRING "" FORCE)
set(LIBEVENT_INCLUDE_DIRS "${LIBEVENT_SRC_DIR}/include;${LIBEVENT_GEN_DIR}" CACHE STRING "" FORCE)
