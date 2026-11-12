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

#include <aws/core/client/ClientConfiguration.h>

#include <exception>
#include <variant>

#include "bos_obj_storage_client.h"
#include "cos_obj_storage_client.h"
#include "cpp/aws_common.h"
#include "cpp/obj-storage/obj_storage_config.h"
#include "gcp_obj_storage_client.h"
#include "obj_storage_sdk.h"
#include "obs_obj_storage_client.h"
#include "oss_obj_storage_client.h"
#include "s3_obj_storage_client.h"
#include "tos_obj_storage_client.h"
#ifdef USE_AZURE
#include "azure_obj_storage_client.h"
#endif

namespace doris {
namespace {

struct BuildClient {
    ObjStorageClientBuildResult operator()(const AwsObjStorageConfig& config) const {
        return S3ObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const OssObjStorageConfig& config) const {
        return OssObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const CosObjStorageConfig& config) const {
        return CosObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const BosObjStorageConfig& config) const {
        return BosObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const ObsObjStorageConfig& config) const {
        return ObsObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const GcpObjStorageConfig& config) const {
        return GcpObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const TosObjStorageConfig& config) const {
        return TosObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const AzureObjStorageConfig& config) const {
#ifdef USE_AZURE
        return AzureObjStorageClient::create(config);
#else
        return {.error = "Azure support is disabled"};
#endif
    }
};

} // namespace

void initialize_obj_storage_sdks(int aws_log_level, int azure_log_level) {
    ensure_aws_sdk_initialized(aws_log_level);
#ifdef USE_AZURE
    initialize_azure_logging(azure_log_level);
#endif
}

int get_default_s3_max_connections() {
    return static_cast<int>(default_aws_client_configuration().maxConnections);
}

ObjStorageClientBuildResult build_obj_storage_client(const ObjStorageClientConfig& config) {
    try {
        return std::visit(BuildClient {}, config);
    } catch (const std::exception& error) {
        return {.error = error.what()};
    }
}

} // namespace doris
