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

#include <variant>

#include "bos_obj_storage_client.h"
#include "cos_obj_storage_client.h"
#include "cpp/obj-storage/obj_storage_config.h"
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

struct BuildClient {
    ObjStorageClientBuildResult operator()(const AwsConfig& config) const {
        return S3ObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const OssConfig& config) const {
        return OssObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const CosConfig& config) const {
        return CosObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const BosConfig& config) const {
        return BosObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const ObsConfig& config) const {
        return ObsObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const GcpConfig& config) const {
        return GcpObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const TosConfig& config) const {
        return TosObjStorageClient::create(config);
    }
    ObjStorageClientBuildResult operator()(const AzureConfig& config) const {
#ifdef USE_AZURE
        return AzureObjStorageClient::create(config);
#else
        return {.error = "Azure support is disabled"};
#endif
    }
};

} // namespace

ObjStorageClientBuildResult build_obj_storage_client(const ObjStorageConfig& config) {
    return std::visit(BuildClient {}, config);
}

} // namespace doris
