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

# Azure SDK for C++ — hand-written add_library (no add_subdirectory)
# Covers: azure-core, azure-identity, azure-storage-common, azure-storage-blobs

set(_AZURE_SRC "${TP_SOURCE_DIR}/azure-sdk-for-cpp-azure-core_1.16.0")

# ---------- azure-core ----------
set(_CORE_DIR "${_AZURE_SRC}/sdk/core/azure-core")

file(GLOB_RECURSE _AZURE_CORE_SRCS "${_CORE_DIR}/src/*.cpp")
# Exclude Windows-only transport; we use curl on Linux
list(FILTER _AZURE_CORE_SRCS EXCLUDE REGEX "/winhttp/")

add_library(azure-core STATIC ${_AZURE_CORE_SRCS})

target_include_directories(azure-core SYSTEM PUBLIC
    ${_CORE_DIR}/inc
)
target_include_directories(azure-core PRIVATE
    ${_CORE_DIR}/src
    ${_CORE_DIR}/src/private
)

target_compile_definitions(azure-core
    PUBLIC  AZ_PLATFORM_POSIX BUILD_CURL_HTTP_TRANSPORT_ADAPTER
    PRIVATE _azure_BUILDING_SDK
)

target_compile_options(azure-core PRIVATE -fPIC -w)

find_package(Threads REQUIRED)
target_link_libraries(azure-core
    PUBLIC  libcurl OpenSSL::SSL
    PRIVATE Threads::Threads
)

set_target_properties(azure-core PROPERTIES
    OUTPUT_NAME azure-core
    POSITION_INDEPENDENT_CODE ON
)

add_library(Azure::azure-core ALIAS azure-core)

# ---------- azure-identity ----------
set(_IDENTITY_DIR "${_AZURE_SRC}/sdk/identity/azure-identity")

file(GLOB_RECURSE _AZURE_IDENTITY_SRCS "${_IDENTITY_DIR}/src/*.cpp")

add_library(azure-identity STATIC ${_AZURE_IDENTITY_SRCS})

target_include_directories(azure-identity SYSTEM PUBLIC
    ${_IDENTITY_DIR}/inc
)
target_include_directories(azure-identity PRIVATE
    ${_IDENTITY_DIR}/src
    ${_IDENTITY_DIR}/src/private
)

target_compile_definitions(azure-identity PRIVATE _azure_BUILDING_SDK)
target_compile_options(azure-identity PRIVATE -fPIC -w)

target_link_libraries(azure-identity
    PUBLIC  azure-core
    PRIVATE OpenSSL::Crypto
)

set_target_properties(azure-identity PROPERTIES
    OUTPUT_NAME azure-identity
    POSITION_INDEPENDENT_CODE ON
)

add_library(Azure::azure-identity ALIAS azure-identity)

# ---------- azure-storage-common ----------
set(_STORAGE_COMMON_DIR "${_AZURE_SRC}/sdk/storage/azure-storage-common")

file(GLOB_RECURSE _AZURE_STORAGE_COMMON_SRCS "${_STORAGE_COMMON_DIR}/src/*.cpp")

add_library(azure-storage-common STATIC ${_AZURE_STORAGE_COMMON_SRCS})

target_include_directories(azure-storage-common SYSTEM PUBLIC
    ${_STORAGE_COMMON_DIR}/inc
)
target_include_directories(azure-storage-common SYSTEM PRIVATE
    ${LIBXML2_INCLUDE_DIR}
)
target_include_directories(azure-storage-common PRIVATE
    ${_STORAGE_COMMON_DIR}/src
    ${_STORAGE_COMMON_DIR}/src/private
)

target_compile_definitions(azure-storage-common PRIVATE _azure_BUILDING_SDK)
target_compile_options(azure-storage-common PRIVATE -fPIC -w)

target_link_libraries(azure-storage-common
    PUBLIC  azure-core
    PRIVATE _xml2 OpenSSL::SSL OpenSSL::Crypto
)

set_target_properties(azure-storage-common PROPERTIES
    OUTPUT_NAME azure-storage-common
    POSITION_INDEPENDENT_CODE ON
)

add_library(Azure::azure-storage-common ALIAS azure-storage-common)

# ---------- azure-storage-blobs ----------
set(_STORAGE_BLOBS_DIR "${_AZURE_SRC}/sdk/storage/azure-storage-blobs")

file(GLOB_RECURSE _AZURE_STORAGE_BLOBS_SRCS "${_STORAGE_BLOBS_DIR}/src/*.cpp")

add_library(azure-storage-blobs STATIC ${_AZURE_STORAGE_BLOBS_SRCS})

target_include_directories(azure-storage-blobs SYSTEM PUBLIC
    ${_STORAGE_BLOBS_DIR}/inc
)
target_include_directories(azure-storage-blobs PRIVATE
    ${_STORAGE_BLOBS_DIR}/src
    ${_STORAGE_BLOBS_DIR}/src/private
)

target_compile_definitions(azure-storage-blobs PRIVATE _azure_BUILDING_SDK)
target_compile_options(azure-storage-blobs PRIVATE -fPIC -w)

target_link_libraries(azure-storage-blobs
    PUBLIC azure-storage-common
)

set_target_properties(azure-storage-blobs PROPERTIES
    OUTPUT_NAME azure-storage-blobs
    POSITION_INDEPENDENT_CODE ON
)

add_library(Azure::azure-storage-blobs ALIAS azure-storage-blobs)
