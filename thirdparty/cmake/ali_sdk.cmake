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

# ali_sdk — hand-written add_library (no add_subdirectory)
set(ALI_SDK_SRC_DIR ${TP_SOURCE_DIR}/aliyun-openapi-cpp-sdk-1.36.1586/core)

# ---------- configure_file: Config.h ----------
set(PROJECT_VERSION_MAJOR 1)
set(PROJECT_VERSION_MINOR 36)
set(PROJECT_VERSION_PATCH 1586)

configure_file(
    "${ALI_SDK_SRC_DIR}/src/Config.h.in"
    "${ALI_SDK_SRC_DIR}/include/alibabacloud/core/Config.h"
    @ONLY
)

# ---------- add_library (explicit list from upstream CMakeLists) ----------
add_library(core STATIC
    ${ALI_SDK_SRC_DIR}/src/AlibabaCloud.cc
    ${ALI_SDK_SRC_DIR}/src/AsyncCallerContext.cc
    ${ALI_SDK_SRC_DIR}/src/ClientConfiguration.cc
    ${ALI_SDK_SRC_DIR}/src/CommonClient.cc
    ${ALI_SDK_SRC_DIR}/src/CommonRequest.cc
    ${ALI_SDK_SRC_DIR}/src/CommonResponse.cc
    ${ALI_SDK_SRC_DIR}/src/CoreClient.cc
    ${ALI_SDK_SRC_DIR}/src/Credentials.cc
    ${ALI_SDK_SRC_DIR}/src/CredentialsProvider.cc
    ${ALI_SDK_SRC_DIR}/src/CurlHttpClient.cc
    ${ALI_SDK_SRC_DIR}/src/EcsMetadataFetcher.cc
    ${ALI_SDK_SRC_DIR}/src/EndpointProvider.cc
    ${ALI_SDK_SRC_DIR}/src/Error.cc
    ${ALI_SDK_SRC_DIR}/src/Executor.cc
    ${ALI_SDK_SRC_DIR}/src/HmacSha1Signer.cc
    ${ALI_SDK_SRC_DIR}/src/HttpClient.cc
    ${ALI_SDK_SRC_DIR}/src/HttpMessage.cc
    ${ALI_SDK_SRC_DIR}/src/HttpResponse.cc
    ${ALI_SDK_SRC_DIR}/src/HttpRequest.cc
    ${ALI_SDK_SRC_DIR}/src/InstanceProfileCredentialsProvider.cc
    ${ALI_SDK_SRC_DIR}/src/NetworkProxy.cc
    ${ALI_SDK_SRC_DIR}/src/Outcome.cc
    ${ALI_SDK_SRC_DIR}/src/Runnable.cc
    ${ALI_SDK_SRC_DIR}/src/RoaServiceClient.cc
    ${ALI_SDK_SRC_DIR}/src/RoaServiceRequest.cc
    ${ALI_SDK_SRC_DIR}/src/RpcServiceClient.cc
    ${ALI_SDK_SRC_DIR}/src/RpcServiceRequest.cc
    ${ALI_SDK_SRC_DIR}/src/ServiceRequest.cc
    ${ALI_SDK_SRC_DIR}/src/ServiceResult.cc
    ${ALI_SDK_SRC_DIR}/src/Signer.cc
    ${ALI_SDK_SRC_DIR}/src/SimpleCredentialsProvider.cc
    ${ALI_SDK_SRC_DIR}/src/StsAssumeRoleCredentialsProvider.cc
    ${ALI_SDK_SRC_DIR}/src/Url.cc
    ${ALI_SDK_SRC_DIR}/src/Utils.cc
    ${ALI_SDK_SRC_DIR}/src/location/LocationClient.cc
    ${ALI_SDK_SRC_DIR}/src/location/LocationRequest.cc
    ${ALI_SDK_SRC_DIR}/src/location/model/DescribeEndpointsRequest.cc
    ${ALI_SDK_SRC_DIR}/src/location/model/DescribeEndpointsResult.cc
    ${ALI_SDK_SRC_DIR}/src/sts/StsClient.cc
    ${ALI_SDK_SRC_DIR}/src/sts/StsRequest.cc
    ${ALI_SDK_SRC_DIR}/src/sts/model/AssumeRoleRequest.cc
    ${ALI_SDK_SRC_DIR}/src/sts/model/AssumeRoleResult.cc
    ${ALI_SDK_SRC_DIR}/src/sts/model/GetCallerIdentityRequest.cc
    ${ALI_SDK_SRC_DIR}/src/sts/model/GetCallerIdentityResult.cc
)

target_include_directories(core SYSTEM PUBLIC
    ${ALI_SDK_SRC_DIR}/include
)

target_include_directories(core PRIVATE
    ${TP_SOURCE_DIR}/jsoncpp-1.9.5/include
    ${TP_SOURCE_DIR}/curl-8.2.1/include
    ${CMAKE_CURRENT_BINARY_DIR}/openssl/include
)

# uuid shim: uuid/uuid.h
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/ali_sdk_uuid_shim/uuid")
file(COPY "${TP_SOURCE_DIR}/libuuid-1.0.3/uuid.h"
     DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/ali_sdk_uuid_shim/uuid")
target_include_directories(core PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/ali_sdk_uuid_shim
)

target_compile_options(core PRIVATE -fPIC -w)
target_compile_definitions(core PRIVATE CURL_STATICLIB)

set_target_properties(core PROPERTIES
    OUTPUT_NAME alibabacloud-sdk-core
    POSITION_INDEPENDENT_CODE ON
)
