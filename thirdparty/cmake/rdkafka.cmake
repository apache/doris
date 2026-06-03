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

# rdkafka — hand-written add_library (no add_subdirectory)
set(RDKAFKA_SRC_DIR ${TP_SOURCE_DIR}/librdkafka-2.11.0)

# Create OpenSSL::SSL and OpenSSL::Crypto IMPORTED targets if they don't exist
if(NOT TARGET OpenSSL::SSL)
    add_library(OpenSSL::SSL STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::SSL PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libssl.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}/openssl/include"
    )
endif()
if(NOT TARGET OpenSSL::Crypto)
    add_library(OpenSSL::Crypto STATIC IMPORTED GLOBAL)
    set_target_properties(OpenSSL::Crypto PROPERTIES
        IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/openssl/lib/libcrypto.a"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}/openssl/include"
    )
endif()

# ---------- embed pre-generated config.h ----------
# Place config.h so that #include "../config.h" from src/*.c resolves correctly
# When compiling src/rd.h, it does #include "../config.h", so config.h must be
# at the parent of whatever include path contains "src/". We put it at the
# rdkafka source root level.
file(WRITE "${RDKAFKA_SRC_DIR}/config.h"
"#define WITHOUT_OPTIMIZATION 0\n"
"#define ENABLE_DEVEL 0\n"
"#define ENABLE_REFCNT_DEBUG 0\n"
"#define HAVE_ATOMICS_32 1\n"
"#define HAVE_ATOMICS_32_SYNC 0\n"
"#if (HAVE_ATOMICS_32)\n"
"# if (HAVE_ATOMICS_32_SYNC)\n"
"#  define ATOMIC_OP32(OP1,OP2,PTR,VAL) __sync_ ## OP1 ## _and_ ## OP2(PTR, VAL)\n"
"# else\n"
"#  define ATOMIC_OP32(OP1,OP2,PTR,VAL) __atomic_ ## OP1 ## _ ## OP2(PTR, VAL, __ATOMIC_SEQ_CST)\n"
"# endif\n"
"#endif\n"
"#define HAVE_ATOMICS_64 1\n"
"#define HAVE_ATOMICS_64_SYNC 0\n"
"#if (HAVE_ATOMICS_64)\n"
"# if (HAVE_ATOMICS_64_SYNC)\n"
"#  define ATOMIC_OP64(OP1,OP2,PTR,VAL) __sync_ ## OP1 ## _and_ ## OP2(PTR, VAL)\n"
"# else\n"
"#  define ATOMIC_OP64(OP1,OP2,PTR,VAL) __atomic_ ## OP1 ## _ ## OP2(PTR, VAL, __ATOMIC_SEQ_CST)\n"
"# endif\n"
"#endif\n"
"#define WITH_PKGCONFIG 1\n"
"#define WITH_HDRHISTOGRAM 1\n"
"#define WITH_ZLIB 1\n"
"#define WITH_CURL 1\n"
"#define WITH_OAUTHBEARER_OIDC 1\n"
"#define WITH_ZSTD 1\n"
"#define WITH_LIBDL 1\n"
"#define WITH_PLUGINS 0\n"
"#define WITH_SNAPPY 1\n"
"#define WITH_SOCKEM 1\n"
"#define WITH_SSL 1\n"
"#define WITH_SASL 1\n"
"#define WITH_SASL_SCRAM 1\n"
"#define WITH_SASL_OAUTHBEARER 1\n"
"#define WITH_SASL_CYRUS 1\n"
"#define WITH_LZ4_EXT 1\n"
"#define HAVE_REGEX 1\n"
"#define HAVE_STRNDUP 1\n"
"#define HAVE_RAND_R 1\n"
"#define HAVE_PTHREAD_SETNAME_GNU 1\n"
"#define HAVE_PTHREAD_SETNAME_DARWIN 0\n"
"#define HAVE_PTHREAD_SETNAME_FREEBSD 0\n"
"#define WITH_C11THREADS 0\n"
"#define WITH_CRC32C_HW 1\n"
"#define SOLIB_EXT \".so\"\n"
"#define BUILT_WITH \"HAND_WRITTEN\"\n"
)

# ---------- rdkafka (C library) ----------
file(GLOB RDKAFKA_C_SRCS "${RDKAFKA_SRC_DIR}/src/*.c")
file(GLOB RDKAFKA_NANOPB_SRCS "${RDKAFKA_SRC_DIR}/src/nanopb/*.c")
file(GLOB RDKAFKA_OTEL_SRCS "${RDKAFKA_SRC_DIR}/src/opentelemetry/*.c")
list(APPEND RDKAFKA_C_SRCS ${RDKAFKA_NANOPB_SRCS} ${RDKAFKA_OTEL_SRCS})
list(FILTER RDKAFKA_C_SRCS EXCLUDE REGEX "/rdkafka_plugin\\.c$")
list(FILTER RDKAFKA_C_SRCS EXCLUDE REGEX "_win32\\.c$")
# Also add the hdrhistogram bundled source
if(EXISTS "${RDKAFKA_SRC_DIR}/src/rdkafka_hdrhistogram.c")
    # already in GLOB
endif()

add_library(rdkafka STATIC ${RDKAFKA_C_SRCS})

target_include_directories(rdkafka SYSTEM PUBLIC
    ${RDKAFKA_SRC_DIR}/src
)

target_include_directories(rdkafka PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/rdkafka_gen
    ${RDKAFKA_SRC_DIR}/src
    ${RDKAFKA_SRC_DIR}
    ${TP_SOURCE_DIR}/zstd-1.5.7/lib
    ${TP_SOURCE_DIR}/lz4-1.9.4/lib
    ${CMAKE_CURRENT_BINARY_DIR}/cyrus-sasl/include
    ${TP_SOURCE_DIR}/cyrus-sasl-2.1.27/include
    ${TP_SOURCE_DIR}/curl-8.2.1/include
)

target_compile_options(rdkafka PRIVATE -fPIC -w)
target_compile_definitions(rdkafka PRIVATE LIBRDKAFKA_STATICLIB)

find_package(Threads REQUIRED)
target_link_libraries(rdkafka PRIVATE
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
    libzstd_static
    lz4_static
    snappy
    zlibstatic
    cyrus-sasl
    ${CMAKE_DL_LIBS}
)

if(TARGET cyrus_sasl_builder)
    add_dependencies(rdkafka cyrus_sasl_builder)
endif()

set_target_properties(rdkafka PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# ---------- rdkafka++ (C++ wrapper) ----------
file(GLOB RDKAFKA_CPP_SRCS "${RDKAFKA_SRC_DIR}/src-cpp/*.cpp")

add_library(rdkafka++ STATIC ${RDKAFKA_CPP_SRCS})

target_include_directories(rdkafka++ SYSTEM PUBLIC
    ${RDKAFKA_SRC_DIR}/src-cpp
)

target_link_libraries(rdkafka++ PUBLIC rdkafka)
target_compile_options(rdkafka++ PRIVATE -fPIC -w)
target_compile_definitions(rdkafka++ PRIVATE LIBRDKAFKA_STATICLIB)

set_target_properties(rdkafka++ PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

add_library(rdkafka_cpp ALIAS rdkafka++)

# ---------- nested header directory ----------
set(RDKAFKA_NESTED_DIR ${CMAKE_CURRENT_BINARY_DIR}/rdkafka_headers/include/librdkafka)
file(MAKE_DIRECTORY ${RDKAFKA_NESTED_DIR})
file(GLOB _RDKAFKA_PUB_H "${RDKAFKA_SRC_DIR}/src/rdkafka.h")
file(GLOB _RDKAFKA_CPP_H "${RDKAFKA_SRC_DIR}/src-cpp/rdkafkacpp.h")
if(_RDKAFKA_PUB_H)
    file(COPY ${_RDKAFKA_PUB_H} DESTINATION ${RDKAFKA_NESTED_DIR})
endif()
if(_RDKAFKA_CPP_H)
    file(COPY ${_RDKAFKA_CPP_H} DESTINATION ${RDKAFKA_NESTED_DIR})
endif()
add_library(rdkafka_headers INTERFACE)
target_include_directories(rdkafka_headers SYSTEM INTERFACE
    ${CMAKE_CURRENT_BINARY_DIR}/rdkafka_headers/include
)

# ZSTD/LZ4 IMPORTED targets for downstream compatibility
if(NOT TARGET ZSTD::ZSTD)
    add_library(ZSTD::ZSTD ALIAS libzstd_static)
endif()
if(NOT TARGET LZ4::LZ4)
    add_library(LZ4::LZ4 ALIAS lz4_static)
endif()
