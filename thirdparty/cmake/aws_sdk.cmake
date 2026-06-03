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

# AWS SDK for C++ — hand-written add_library (no add_subdirectory)
# Builds the exact targets Doris needs from source without invoking the AWS SDK's
# CMake machinery, which requires many feature-probe try_compile() runs.
#
# Targets produced:
#   C runtime (pure C):
#     aws-c-common, aws-c-sdkutils, aws-c-compression, aws-checksums,
#     aws-c-cal, aws-c-io, aws-c-http, aws-c-auth, aws-c-mqtt,
#     aws-c-event-stream, aws-c-s3, s2n, aws-crt-cpp (C++)
#   SDK (C++):
#     aws-cpp-sdk-core, aws-cpp-sdk-s3, aws-cpp-sdk-s3-crt,
#     aws-cpp-sdk-transfer, aws-cpp-sdk-sts,
#     aws-cpp-sdk-identity-management
#   Interface helper:
#     aws_c_headers  (exposes all CRT include paths transitively)

# ============================================================================
# Paths
# ============================================================================
set(AWS_SDK_SRC  ${TP_SOURCE_DIR}/aws-sdk-cpp-1.11.219)
set(AWS_CRT_ROOT ${AWS_SDK_SRC}/crt/aws-crt-cpp/crt)
set(AWS_CRT_CPP  ${AWS_SDK_SRC}/crt/aws-crt-cpp)

# Build-time generated-include directory for aws-c-common/config.h
# We copy the pre-built config.h from the reference build into a stable location.
set(AWS_GEN_DIR  ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-gen)
set(AWS_COMMON_GEN_INCLUDE ${AWS_GEN_DIR}/aws-c-common/generated/include)
set(AWS_CRT_CPP_GEN_INCLUDE ${AWS_GEN_DIR}/aws-crt-cpp/generated/include)
file(MAKE_DIRECTORY ${AWS_COMMON_GEN_INCLUDE}/aws/common)
file(MAKE_DIRECTORY ${AWS_CRT_CPP_GEN_INCLUDE}/aws/crt)

# config.h.in uses #cmakedefine — generate a concrete header for Linux x86_64
set(AWS_HAVE_GCC_OVERFLOW_MATH_EXTENSIONS 1)
set(AWS_HAVE_GCC_INLINE_ASM 1)
set(AWS_HAVE_MSVC_INTRINSICS_X64)
set(AWS_HAVE_POSIX_LARGE_FILE_SUPPORT 1)
set(AWS_HAVE_EXECINFO 1)
set(AWS_HAVE_WINAPI_DESKTOP)
set(AWS_HAVE_LINUX_IF_LINK_H 1)
configure_file(
    ${AWS_CRT_ROOT}/aws-c-common/include/aws/common/config.h.in
    ${AWS_COMMON_GEN_INCLUDE}/aws/common/config.h
    @ONLY
)

# Generate aws-cpp-sdk-core's SDKConfig.h from its .in template.
# The pristine aws-sdk-cpp tarball does not ship this header (it's a
# cmakedefine-only template, normally produced by upstream's CMake during
# configure). Without it, every aws-cpp-sdk-core TU fails with
# "fatal error: 'aws/core/SDKConfig.h' file not found".
set(AWS_CPP_SDK_CORE_GEN_INCLUDE ${AWS_GEN_DIR}/aws-cpp-sdk-core/include)
file(MAKE_DIRECTORY ${AWS_CPP_SDK_CORE_GEN_INCLUDE}/aws/core)
# Doris does not enable AWS_MEMORY_MANAGEMENT custom allocator; leave undefined.
configure_file(
    ${AWS_SDK_SRC}/src/aws-cpp-sdk-core/include/aws/core/SDKConfig.h.in
    ${AWS_CPP_SDK_CORE_GEN_INCLUDE}/aws/core/SDKConfig.h
)

# Generate aws-crt-cpp Config.h from Config.h.in
# The template uses @VAR@ substitution
set(FULL_VERSION "0.24.8")
set(PROJECT_VERSION_MAJOR 0)
set(PROJECT_VERSION_MINOR 24)
set(PROJECT_VERSION_PATCH 8)
set(GIT_HASH "")
configure_file(
    ${AWS_CRT_CPP}/include/aws/crt/Config.h.in
    ${AWS_CRT_CPP_GEN_INCLUDE}/aws/crt/Config.h
    @ONLY
)

# ============================================================================
# CURL::libcurl alias (needed by aws-cpp-sdk-core)
# ============================================================================
if(TARGET libcurl AND NOT TARGET CURL::libcurl)
    add_library(CURL::libcurl ALIAS libcurl)
endif()

# ============================================================================
# Common compile options helper
# ============================================================================
# C libraries: fPIC, suppress warnings (we're not their author)
set(_AWS_C_OPTS   -fPIC -w)
set(_AWS_CXX_OPTS -fPIC -w)

# ============================================================================
# aws-c-common
# ============================================================================
file(GLOB _COMMON_SRC
    "${AWS_CRT_ROOT}/aws-c-common/source/*.c"
    "${AWS_CRT_ROOT}/aws-c-common/source/external/*.c"
    "${AWS_CRT_ROOT}/aws-c-common/source/posix/*.c"
    "${AWS_CRT_ROOT}/aws-c-common/source/linux/*.c"
)
# On x86_64 use intel cpuid (which provides aws_cpu_has_feature) and the
# inline-asm helper (aws_run_cpuid). Do NOT include generic/cpuid.c — it
# provides the same symbol with a stub implementation and would cause a
# duplicate-symbol link error.
list(APPEND _COMMON_SRC
    "${AWS_CRT_ROOT}/aws-c-common/source/arch/intel/cpuid.c"
    "${AWS_CRT_ROOT}/aws-c-common/source/arch/intel/asm/cpuid.c"
)
# Exclude anything from windows/darwin/android/tests
list(FILTER _COMMON_SRC EXCLUDE REGEX "/windows/|/darwin/|/android/|/tests/")

add_library(aws-c-common STATIC ${_COMMON_SRC})
target_include_directories(aws-c-common SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-common/include
    ${AWS_COMMON_GEN_INCLUDE}
)
target_compile_options(aws-c-common PRIVATE ${_AWS_C_OPTS})
target_compile_definitions(aws-c-common PRIVATE
    _POSIX_C_SOURCE=200809L
    _XOPEN_SOURCE=500
    _GNU_SOURCE
    CJSON_HIDE_SYMBOLS
    AWS_AFFINITY_METHOD=2
    AWS_HAVE_GCC_OVERFLOW_MATH_EXTENSIONS
    AWS_HAVE_GCC_INLINE_ASM
    AWS_HAVE_POSIX_LARGE_FILE_SUPPORT
    AWS_HAVE_EXECINFO
    AWS_HAVE_LINUX_IF_LINK_H
    INTEL_NO_ITTNOTIFY_API
)
# Propagate INTEL_NO_ITTNOTIFY_API to all consumers (aws-c-io's tracing.c calls __itt_*)
target_compile_definitions(aws-c-common PUBLIC INTEL_NO_ITTNOTIFY_API)
target_link_libraries(aws-c-common PUBLIC pthread dl m rt)
set_target_properties(aws-c-common PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-c-sdkutils
# ============================================================================
file(GLOB _SDKUTILS_SRC
    "${AWS_CRT_ROOT}/aws-c-sdkutils/source/*.c"
)

add_library(aws-c-sdkutils STATIC ${_SDKUTILS_SRC})
target_include_directories(aws-c-sdkutils SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-sdkutils/include
)
target_compile_options(aws-c-sdkutils PRIVATE ${_AWS_C_OPTS})
target_link_libraries(aws-c-sdkutils PUBLIC aws-c-common)
set_target_properties(aws-c-sdkutils PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-c-compression
# ============================================================================
file(GLOB _COMPRESSION_SRC
    "${AWS_CRT_ROOT}/aws-c-compression/source/*.c"
)
# Exclude the huffman generator (it's a tool, not a library source)
list(FILTER _COMPRESSION_SRC EXCLUDE REGEX "/huffman_generator/")

add_library(aws-c-compression STATIC ${_COMPRESSION_SRC})
target_include_directories(aws-c-compression SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-compression/include
)
target_compile_options(aws-c-compression PRIVATE ${_AWS_C_OPTS})
target_link_libraries(aws-c-compression PUBLIC aws-c-common)
set_target_properties(aws-c-compression PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-checksums
# ============================================================================
# Use the generic (portable C) CRC implementation; the intel/asm variant
# requires inline-assembly feature detection that we skip here.
file(GLOB _CHECKSUMS_SRC
    "${AWS_CRT_ROOT}/aws-checksums/source/*.c"
    "${AWS_CRT_ROOT}/aws-checksums/source/generic/*.c"
)

add_library(aws-checksums STATIC ${_CHECKSUMS_SRC})
target_include_directories(aws-checksums SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-checksums/include
)
target_compile_options(aws-checksums PRIVATE ${_AWS_C_OPTS})
target_link_libraries(aws-checksums PUBLIC aws-c-common)
set_target_properties(aws-checksums PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# s2n  (TLS library; required by aws-c-io on Linux)
# ============================================================================
set(_S2N_DIR ${AWS_CRT_ROOT}/s2n)

file(GLOB_RECURSE _S2N_TLS_SRC    "${_S2N_DIR}/tls/*.c")
file(GLOB         _S2N_CRYPTO_SRC "${_S2N_DIR}/crypto/*.c")
file(GLOB         _S2N_STUFFER_SRC "${_S2N_DIR}/stuffer/*.c")
file(GLOB         _S2N_UTILS_SRC  "${_S2N_DIR}/utils/*.c")
file(GLOB         _S2N_ERROR_SRC  "${_S2N_DIR}/error/*.c")
file(GLOB         _S2N_PQ_SRC     "${_S2N_DIR}/pq-crypto/*.c")
file(GLOB         _S2N_PQ_KYBER_SRC "${_S2N_DIR}/pq-crypto/kyber_r3/*.c")
# Exclude the AVX2-optimized Kyber C files — they call into .S assembly that
# we do not include (no ASM support in this hand-written build).  The non-AVX2
# portable implementations in the same directory cover the same functionality.
list(FILTER _S2N_PQ_KYBER_SRC EXCLUDE REGEX "_avx2\\.c$")
# Exclude test files
list(FILTER _S2N_TLS_SRC    EXCLUDE REGEX "/tests/")
list(FILTER _S2N_CRYPTO_SRC EXCLUDE REGEX "/tests/")
list(FILTER _S2N_STUFFER_SRC EXCLUDE REGEX "/tests/")
list(FILTER _S2N_UTILS_SRC  EXCLUDE REGEX "/tests/")
list(FILTER _S2N_ERROR_SRC  EXCLUDE REGEX "/tests/")

set(_S2N_SRCS
    ${_S2N_TLS_SRC}
    ${_S2N_CRYPTO_SRC}
    ${_S2N_STUFFER_SRC}
    ${_S2N_UTILS_SRC}
    ${_S2N_ERROR_SRC}
    ${_S2N_PQ_SRC}
    ${_S2N_PQ_KYBER_SRC}
)

add_library(s2n STATIC ${_S2N_SRCS})
target_include_directories(s2n SYSTEM PUBLIC
    ${_S2N_DIR}/api
    ${_S2N_DIR}
)
target_include_directories(s2n PRIVATE
    ${_S2N_DIR}/crypto
    ${_S2N_DIR}/tls
    ${_S2N_DIR}/utils
    ${_S2N_DIR}/stuffer
    ${_S2N_DIR}/error
    ${_S2N_DIR}/pq-crypto
)
target_compile_options(s2n PRIVATE
    ${_AWS_C_OPTS}
    -std=gnu99
    -fvisibility=hidden
)
target_compile_definitions(s2n PRIVATE
    _POSIX_C_SOURCE=200809L
    S2N_EXPORTS
)
target_link_libraries(s2n PUBLIC _crypto pthread dl rt)
set_target_properties(s2n PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-c-cal  (Crypto Abstraction Layer; uses OpenSSL on Linux)
# ============================================================================
file(GLOB _CAL_SRC
    "${AWS_CRT_ROOT}/aws-c-cal/source/*.c"
    "${AWS_CRT_ROOT}/aws-c-cal/source/unix/*.c"
)
list(FILTER _CAL_SRC EXCLUDE REGEX "/darwin/|/windows/")

add_library(aws-c-cal STATIC ${_CAL_SRC})
target_include_directories(aws-c-cal SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-cal/include
)
target_compile_options(aws-c-cal PRIVATE ${_AWS_C_OPTS})
target_link_libraries(aws-c-cal PUBLIC aws-c-common _crypto)
set_target_properties(aws-c-cal PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-c-io  (I/O library; epoll on Linux, s2n for TLS)
# ============================================================================
file(GLOB _IO_SRC
    "${AWS_CRT_ROOT}/aws-c-io/source/*.c"
    "${AWS_CRT_ROOT}/aws-c-io/source/posix/*.c"
    "${AWS_CRT_ROOT}/aws-c-io/source/linux/*.c"
    "${AWS_CRT_ROOT}/aws-c-io/source/s2n/*.c"
)
# Explicitly exclude non-Linux platform directories (darwin, windows, bsd)
list(FILTER _IO_SRC EXCLUDE REGEX "/darwin/|/windows/|/bsd/")
# source/tls_channel_handler.c (generic) and source/s2n/s2n_tls_channel_handler.c
# both define aws_tls_key_operation_* and aws_tls_client_ctx_new etc.
# The upstream cmake combines them in one target and relies on the linker
# accepting the first definition. Use link option to allow this with lld.

add_library(aws-c-io STATIC ${_IO_SRC})
target_include_directories(aws-c-io SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-io/include
)
target_compile_options(aws-c-io PRIVATE ${_AWS_C_OPTS})
target_compile_definitions(aws-c-io PUBLIC AWS_USE_EPOLL)
# --allow-multiple-definition: tls_channel_handler.c and s2n_tls_channel_handler.c
# both define aws_tls_key_operation_* (upstream design, works with GNU ld but not lld)
target_link_options(aws-c-io INTERFACE -Wl,--allow-multiple-definition)
target_link_libraries(aws-c-io PUBLIC aws-c-common aws-c-cal s2n)
set_target_properties(aws-c-io PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-c-http
# ============================================================================
file(GLOB _HTTP_SRC
    "${AWS_CRT_ROOT}/aws-c-http/source/*.c"
)

add_library(aws-c-http STATIC ${_HTTP_SRC})
target_include_directories(aws-c-http SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-http/include
)
target_compile_options(aws-c-http PRIVATE ${_AWS_C_OPTS})
target_link_libraries(aws-c-http PUBLIC aws-c-io aws-c-compression)
set_target_properties(aws-c-http PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-c-auth
# ============================================================================
file(GLOB _AUTH_SRC
    "${AWS_CRT_ROOT}/aws-c-auth/source/*.c"
)

add_library(aws-c-auth STATIC ${_AUTH_SRC})
target_include_directories(aws-c-auth SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-auth/include
)
target_compile_options(aws-c-auth PRIVATE ${_AWS_C_OPTS})
target_link_libraries(aws-c-auth PUBLIC aws-c-sdkutils aws-c-cal aws-c-http)
set_target_properties(aws-c-auth PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-c-mqtt
# ============================================================================
file(GLOB _MQTT_SRC
    "${AWS_CRT_ROOT}/aws-c-mqtt/source/*.c"
    "${AWS_CRT_ROOT}/aws-c-mqtt/source/v5/*.c"
)

add_library(aws-c-mqtt STATIC ${_MQTT_SRC})
target_include_directories(aws-c-mqtt SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-mqtt/include
)
target_compile_options(aws-c-mqtt PRIVATE ${_AWS_C_OPTS})
target_link_libraries(aws-c-mqtt PUBLIC aws-c-http)
set_target_properties(aws-c-mqtt PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-c-event-stream
# ============================================================================
file(GLOB _EVENT_STREAM_SRC
    "${AWS_CRT_ROOT}/aws-c-event-stream/source/*.c"
)

add_library(aws-c-event-stream STATIC ${_EVENT_STREAM_SRC})
target_include_directories(aws-c-event-stream SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-event-stream/include
)
target_compile_options(aws-c-event-stream PRIVATE ${_AWS_C_OPTS})
target_link_libraries(aws-c-event-stream PUBLIC aws-c-io aws-c-common aws-checksums)
set_target_properties(aws-c-event-stream PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-c-s3
# ============================================================================
file(GLOB _C_S3_SRC
    "${AWS_CRT_ROOT}/aws-c-s3/source/*.c"
    "${AWS_CRT_ROOT}/aws-c-s3/source/s3_endpoint_resolver/*.c"
)

add_library(aws-c-s3 STATIC ${_C_S3_SRC})
target_include_directories(aws-c-s3 SYSTEM PUBLIC
    ${AWS_CRT_ROOT}/aws-c-s3/include
)
target_compile_options(aws-c-s3 PRIVATE ${_AWS_C_OPTS})
target_link_libraries(aws-c-s3 PUBLIC aws-c-auth aws-checksums)
set_target_properties(aws-c-s3 PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-crt-cpp  (C++ CRT wrapper)
# ============================================================================
file(GLOB _CRT_CPP_SRC
    "${AWS_CRT_CPP}/source/*.cpp"
    "${AWS_CRT_CPP}/source/auth/*.cpp"
    "${AWS_CRT_CPP}/source/crypto/*.cpp"
    "${AWS_CRT_CPP}/source/io/*.cpp"
    "${AWS_CRT_CPP}/source/iot/*.cpp"
    "${AWS_CRT_CPP}/source/mqtt/*.cpp"
    "${AWS_CRT_CPP}/source/http/*.cpp"
    "${AWS_CRT_CPP}/source/endpoints/*.cpp"
    "${AWS_CRT_CPP}/source/external/*.cpp"
)

add_library(aws-crt-cpp STATIC ${_CRT_CPP_SRC})
target_include_directories(aws-crt-cpp SYSTEM PUBLIC
    ${AWS_CRT_CPP}/include
    ${AWS_CRT_CPP_GEN_INCLUDE}
)
target_compile_options(aws-crt-cpp PRIVATE ${_AWS_CXX_OPTS})
target_compile_definitions(aws-crt-cpp PRIVATE CJSON_HIDE_SYMBOLS)
target_link_libraries(aws-crt-cpp PUBLIC
    aws-c-http
    aws-c-mqtt
    aws-c-cal
    aws-c-auth
    aws-c-common
    aws-c-io
    aws-c-s3
    aws-checksums
    aws-c-event-stream
)
set_target_properties(aws-crt-cpp PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-cpp-sdk-core
# ============================================================================
file(GLOB_RECURSE _CORE_SRC
    "${AWS_SDK_SRC}/src/aws-cpp-sdk-core/source/*.cpp"
)
# Keep only Linux-compatible platform sources; exclude windows/ and android/
list(FILTER _CORE_SRC EXCLUDE REGEX "/source/platform/windows/|/source/platform/android/")
list(FILTER _CORE_SRC EXCLUDE REGEX "/source/net/windows/")
# Exclude bcrypt and commoncrypto encryption backends (Linux uses OpenSSL)
list(FILTER _CORE_SRC EXCLUDE REGEX "/source/utils/crypto/bcrypt/|/source/utils/crypto/commoncrypto/")
# Exclude the CRT HTTP client (we use curl)
list(FILTER _CORE_SRC EXCLUDE REGEX "/source/http/crt/")
# Exclude Windows HTTP client
list(FILTER _CORE_SRC EXCLUDE REGEX "/source/http/windows/")
# Exclude OpenTelemetry tracing (requires opentelemetry-cpp which is not available)
list(FILTER _CORE_SRC EXCLUDE REGEX "/opentelemetry/")

add_library(aws-cpp-sdk-core STATIC ${_CORE_SRC})
target_include_directories(aws-cpp-sdk-core SYSTEM PUBLIC
    ${AWS_SDK_SRC}/src/aws-cpp-sdk-core/include
    # Generated SDKConfig.h
    ${AWS_CPP_SDK_CORE_GEN_INCLUDE}
    # CRT headers needed by aws/crt/Allocator.h etc.
    ${AWS_CRT_CPP}/include
    ${AWS_CRT_CPP_GEN_INCLUDE}
    # aws-c-common generated config.h
    ${AWS_COMMON_GEN_INCLUDE}
    # All CRT C library includes
    ${AWS_CRT_ROOT}/aws-c-common/include
    ${AWS_CRT_ROOT}/aws-c-auth/include
    ${AWS_CRT_ROOT}/aws-c-cal/include
    ${AWS_CRT_ROOT}/aws-c-compression/include
    ${AWS_CRT_ROOT}/aws-c-event-stream/include
    ${AWS_CRT_ROOT}/aws-c-http/include
    ${AWS_CRT_ROOT}/aws-c-io/include
    ${AWS_CRT_ROOT}/aws-c-mqtt/include
    ${AWS_CRT_ROOT}/aws-c-s3/include
    ${AWS_CRT_ROOT}/aws-c-sdkutils/include
    ${AWS_CRT_ROOT}/aws-checksums/include
)
target_compile_options(aws-cpp-sdk-core PRIVATE ${_AWS_CXX_OPTS})
target_compile_definitions(aws-cpp-sdk-core PUBLIC
    ENABLE_CURL_CLIENT
    ENABLE_OPENSSL_ENCRYPTION
)
target_link_libraries(aws-cpp-sdk-core PUBLIC
    aws-crt-cpp
    aws-c-event-stream
    CURL::libcurl
    _ssl
    _crypto
    z
    pthread
    dl
    rt
)
set_target_properties(aws-cpp-sdk-core PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-cpp-sdk-s3
# ============================================================================
file(GLOB_RECURSE _S3_SRC
    "${AWS_SDK_SRC}/generated/src/aws-cpp-sdk-s3/source/*.cpp"
)

add_library(aws-cpp-sdk-s3 STATIC ${_S3_SRC})
target_include_directories(aws-cpp-sdk-s3 SYSTEM PUBLIC
    ${AWS_SDK_SRC}/generated/src/aws-cpp-sdk-s3/include
)
target_compile_options(aws-cpp-sdk-s3 PRIVATE ${_AWS_CXX_OPTS})
target_link_libraries(aws-cpp-sdk-s3 PUBLIC aws-cpp-sdk-core)
set_target_properties(aws-cpp-sdk-s3 PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-cpp-sdk-s3-crt
# ============================================================================
file(GLOB_RECURSE _S3_CRT_SRC
    "${AWS_SDK_SRC}/generated/src/aws-cpp-sdk-s3-crt/source/*.cpp"
)

add_library(aws-cpp-sdk-s3-crt STATIC ${_S3_CRT_SRC})
target_include_directories(aws-cpp-sdk-s3-crt SYSTEM PUBLIC
    ${AWS_SDK_SRC}/generated/src/aws-cpp-sdk-s3-crt/include
)
target_compile_options(aws-cpp-sdk-s3-crt PRIVATE ${_AWS_CXX_OPTS})
target_link_libraries(aws-cpp-sdk-s3-crt PUBLIC aws-cpp-sdk-core aws-c-s3)
set_target_properties(aws-cpp-sdk-s3-crt PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-cpp-sdk-kinesis (used by BE routine_load Kinesis consumer)
# ============================================================================
file(GLOB_RECURSE _KINESIS_SRC
    "${AWS_SDK_SRC}/generated/src/aws-cpp-sdk-kinesis/source/*.cpp"
)

add_library(aws-cpp-sdk-kinesis STATIC ${_KINESIS_SRC})
target_include_directories(aws-cpp-sdk-kinesis SYSTEM PUBLIC
    ${AWS_SDK_SRC}/generated/src/aws-cpp-sdk-kinesis/include
)
target_compile_options(aws-cpp-sdk-kinesis PRIVATE ${_AWS_CXX_OPTS})
target_link_libraries(aws-cpp-sdk-kinesis PUBLIC aws-cpp-sdk-core)
set_target_properties(aws-cpp-sdk-kinesis PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-cpp-sdk-sts
# ============================================================================
file(GLOB_RECURSE _STS_SRC
    "${AWS_SDK_SRC}/generated/src/aws-cpp-sdk-sts/source/*.cpp"
)

add_library(aws-cpp-sdk-sts STATIC ${_STS_SRC})
target_include_directories(aws-cpp-sdk-sts SYSTEM PUBLIC
    ${AWS_SDK_SRC}/generated/src/aws-cpp-sdk-sts/include
)
target_compile_options(aws-cpp-sdk-sts PRIVATE ${_AWS_CXX_OPTS})
target_link_libraries(aws-cpp-sdk-sts PUBLIC aws-cpp-sdk-core)
set_target_properties(aws-cpp-sdk-sts PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-cpp-sdk-identity-management
# ============================================================================
file(GLOB_RECURSE _IDENTITY_SRC
    "${AWS_SDK_SRC}/src/aws-cpp-sdk-identity-management/source/*.cpp"
)

add_library(aws-cpp-sdk-identity-management STATIC ${_IDENTITY_SRC})
target_include_directories(aws-cpp-sdk-identity-management SYSTEM PUBLIC
    ${AWS_SDK_SRC}/src/aws-cpp-sdk-identity-management/include
    ${AWS_SDK_SRC}/generated/src/aws-cpp-sdk-cognito-identity/include
)
target_compile_options(aws-cpp-sdk-identity-management PRIVATE ${_AWS_CXX_OPTS})
target_link_libraries(aws-cpp-sdk-identity-management PUBLIC aws-cpp-sdk-sts)
set_target_properties(aws-cpp-sdk-identity-management PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws-cpp-sdk-transfer
# ============================================================================
file(GLOB_RECURSE _TRANSFER_SRC
    "${AWS_SDK_SRC}/src/aws-cpp-sdk-transfer/source/*.cpp"
)

add_library(aws-cpp-sdk-transfer STATIC ${_TRANSFER_SRC})
target_include_directories(aws-cpp-sdk-transfer SYSTEM PUBLIC
    ${AWS_SDK_SRC}/src/aws-cpp-sdk-transfer/include
)
target_compile_options(aws-cpp-sdk-transfer PRIVATE ${_AWS_CXX_OPTS})
target_link_libraries(aws-cpp-sdk-transfer PUBLIC aws-cpp-sdk-s3)
set_target_properties(aws-cpp-sdk-transfer PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# aws_c_headers — INTERFACE target exposing all CRT C includes
# Used by Doris BE targets that include aws/crt/* headers indirectly.
# ============================================================================
add_library(aws_c_headers INTERFACE)
target_include_directories(aws_c_headers SYSTEM INTERFACE
    ${AWS_CRT_ROOT}/aws-c-auth/include
    ${AWS_CRT_ROOT}/aws-c-cal/include
    ${AWS_CRT_ROOT}/aws-c-common/include
    ${AWS_CRT_ROOT}/aws-c-compression/include
    ${AWS_CRT_ROOT}/aws-c-event-stream/include
    ${AWS_CRT_ROOT}/aws-c-http/include
    ${AWS_CRT_ROOT}/aws-c-io/include
    ${AWS_CRT_ROOT}/aws-c-mqtt/include
    ${AWS_CRT_ROOT}/aws-c-s3/include
    ${AWS_CRT_ROOT}/aws-c-sdkutils/include
    ${AWS_CRT_ROOT}/aws-checksums/include
    ${AWS_COMMON_GEN_INCLUDE}
)
