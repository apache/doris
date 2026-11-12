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

#include "io/object_storage/obj_storage_client_factory.h"

#include <variant>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/aws_common.h"
#include "cpp/obj-storage/client/bos_obj_storage_client.h"
#include "cpp/obj-storage/client/cos_obj_storage_client.h"
#include "cpp/obj-storage/client/gcp_obj_storage_client.h"
#include "cpp/obj-storage/client/obj_storage_client_runtime.h"
#include "cpp/obj-storage/client/obs_obj_storage_client.h"
#include "cpp/obj-storage/client/oss_obj_storage_client.h"
#include "cpp/obj-storage/client/s3_obj_storage_client.h"
#include "cpp/obj-storage/client/tos_obj_storage_client.h"
#include "io/fs/gcs_signed_url_provider.h"
#include "io/object_storage/aws_client_settings.h"
#ifdef USE_AZURE
#include "cpp/obj-storage/client/azure_obj_storage_client.h"
#endif

namespace doris {
namespace {

using ClientResult = Result<std::shared_ptr<io::ObjStorageClient>>;

ClientResult checked_client(ObjStorageClientBuildResult built) {
    if (!built.client) {
        return ResultError(
                Status::InvalidArgument("failed to create object storage client: {}", built.error));
    }
    return std::move(built.client);
}

ObjStorageClientRuntime client_runtime() {
    return {.credential_provider_version = config::aws_credentials_provider_version,
            .empty_credentials = EmptyCredentialsBehavior::ANONYMOUS,
            .sts_request_timeout_ms = config::aws_client_request_timeout_ms,
            .ca_cert_path = be_ca_bundle_path(),
            .http_scheme = config::s3_client_http_scheme,
            .max_retries = config::max_s3_client_retry,
            .retry_slow_down = true,
            .gcs_signer = io::make_gcs_signed_url_generator,
            .azure_log_level = config::azure_log_level,
            .retry_too_many_requests = true,
            .ca_cert_file_paths = config::ca_cert_file_paths};
}

ClientResult create_provider_client(const ObjStorageConfig& storage_config) {
    const auto runtime = client_runtime();
    if (const auto* aws = std::get_if<AwsConfig>(&storage_config)) {
        return checked_client(S3ObjStorageClient::create(*aws, runtime));
    }
    if (const auto* oss = std::get_if<OssConfig>(&storage_config)) {
        return checked_client(OssObjStorageClient::create(*oss, runtime));
    }
    if (const auto* cos = std::get_if<CosConfig>(&storage_config)) {
        return checked_client(CosObjStorageClient::create(*cos, runtime));
    }
    if (const auto* bos = std::get_if<BosConfig>(&storage_config)) {
        return checked_client(BosObjStorageClient::create(*bos, runtime));
    }
    if (const auto* obs = std::get_if<ObsConfig>(&storage_config)) {
        return checked_client(ObsObjStorageClient::create(*obs, runtime));
    }
    if (const auto* gcp = std::get_if<GcpConfig>(&storage_config)) {
        return checked_client(GcpObjStorageClient::create(*gcp, runtime));
    }
    if (const auto* tos = std::get_if<TosConfig>(&storage_config)) {
        return checked_client(TosObjStorageClient::create(*tos, runtime));
    }
    if (const auto* azure = std::get_if<AzureConfig>(&storage_config)) {
#ifdef USE_AZURE
        return checked_client(AzureObjStorageClient::create(*azure, runtime));
#else
        return ResultError(
                Status::NotSupported("BE is not compiled with azure support, export "
                                     "BUILD_AZURE=ON before building"));
#endif
    }
    return ResultError(Status::NotSupported("Unsupported object storage provider"));
}

} // namespace

ObjStorageClientFactory& ObjStorageClientFactory::instance() {
    // Cached clients must be destroyed before the process shuts down the AWS SDK.
    ensure_aws_sdk_initialized(config::aws_log_level);
    static ObjStorageClientFactory factory;
    return factory;
}

Result<std::shared_ptr<io::ObjStorageClient>> ObjStorageClientFactory::create(
        const ObjStorageConfig& storage_config) {
    if (auto error = validate_obj_storage_config(storage_config)) {
        return ResultError(Status::InvalidArgument<false>(*error));
    }

#ifdef BE_TEST
    {
        std::lock_guard l(_lock);
        if (_test_client_creator) {
            return _test_client_creator(storage_config);
        }
    }
#endif

    auto cache_key = obj_storage_client_cache_key(storage_config);
    {
        std::lock_guard l(_lock);
        auto it = _cache.find(cache_key);
        if (it != _cache.end()) {
            return it->second;
        }
    }

    auto client = create_provider_client(storage_config);
    if (!client) {
        return client;
    }
    LOG_INFO("create object storage client with {}", obj_storage_config_to_string(storage_config));

    {
        std::lock_guard l(_lock);
        auto [it, _] = _cache.emplace(std::move(cache_key), std::move(*client));
        return it->second;
    }
}

#ifdef BE_TEST
void ObjStorageClientFactory::set_client_creator_for_test(
        std::function<std::shared_ptr<io::ObjStorageClient>(const ObjStorageConfig&)> creator) {
    std::lock_guard l(_lock);
    _test_client_creator = std::move(creator);
}

void ObjStorageClientFactory::clear_client_creator_for_test() {
    std::lock_guard l(_lock);
    _test_client_creator = nullptr;
}
#endif

} // namespace doris
