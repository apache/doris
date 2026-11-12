// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "obj_storage_client_builder.h"

#include <aws/core/client/ClientConfiguration.h>

#include <exception>

#include "bos_obj_storage_client.h"
#include "cos_obj_storage_client.h"
#include "cpp/aws_common.h"
#include "gcp_obj_storage_client.h"
#include "obs_obj_storage_client.h"
#include "oss_obj_storage_client.h"
#include "s3_obj_storage_client.h"
#include "tos_obj_storage_client.h"
#ifdef USE_AZURE
#include "azure_obj_storage_client.h"
#endif

namespace doris {
namespace {

ObjStorageClientBuildResult build(const AwsObjStorageConfig& config) {
    return S3ObjStorageClient::create(config);
}

ObjStorageClientBuildResult build(const BosObjStorageConfig& config) {
    return BosObjStorageClient::create(config);
}

ObjStorageClientBuildResult build(const CosObjStorageConfig& config) {
    return CosObjStorageClient::create(config);
}

ObjStorageClientBuildResult build(const GcpObjStorageConfig& config) {
    return GcpObjStorageClient::create(config);
}

ObjStorageClientBuildResult build(const ObsObjStorageConfig& config) {
    return ObsObjStorageClient::create(config);
}

ObjStorageClientBuildResult build(const OssObjStorageConfig& config) {
    return OssObjStorageClient::create(config);
}

ObjStorageClientBuildResult build(const TosObjStorageConfig& config) {
    return TosObjStorageClient::create(config);
}

ObjStorageClientBuildResult build(const AzureObjStorageConfig& config) {
#ifdef USE_AZURE
    return AzureObjStorageClient::create(config);
#else
    return {.error = "Azure support is disabled in this build", .unsupported = true};
#endif
}
} // namespace

ObjStorageClientBuildResult build_obj_storage_client(const ObjStorageClientConfig& config) {
    try {
        return std::visit([](const auto& provider) { return build(provider); }, config);
    } catch (const std::exception& ex) {
        return {.error = ex.what()};
    }
}

unsigned default_s3_max_connections() {
    return default_aws_client_configuration().maxConnections;
}

void initialize_obj_storage_sdks(int aws_log_level, int azure_log_level) {
    ensure_aws_sdk_initialized(aws_log_level);
#ifdef USE_AZURE
    initialize_azure_sdk(azure_log_level);
#endif
}

} // namespace doris
