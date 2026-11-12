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

#include <type_traits>
#include <variant>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/aws_common.h"
#include "io/fs/gcs_signed_url_provider.h"
#include "io/object_storage/aws_client_settings.h"

namespace doris {
namespace {

using ClientResult = Result<std::shared_ptr<io::ObjStorageClient>>;

ClientResult create_provider_client(ObjStorageConfig storage_config) {
#ifndef USE_AZURE
    if (std::holds_alternative<AzureConfig>(storage_config)) {
        return ResultError(
                Status::NotSupported("BE is not compiled with azure support, export "
                                     "BUILD_AZURE=ON before building"));
    }
#endif
    std::visit(
            []<typename Config>(Config& provider) {
                if constexpr (std::is_same_v<Config, AzureConfig>) {
                    provider.azure_log_level = config::azure_log_level;
                    provider.max_retries = config::max_s3_client_retry;
                    provider.retry_too_many_requests = true;
                    provider.ca_cert_path = be_ca_bundle_path();
                    provider.ca_cert_file_paths = config::ca_cert_file_paths;
                } else {
                    provider.s3.credential_provider_version =
                            config::aws_credentials_provider_version;
                    provider.s3.empty_credentials = EmptyCredentialsBehavior::ANONYMOUS;
                    provider.s3.ca_cert_path = be_ca_bundle_path();
                    provider.s3.http_scheme = config::s3_client_http_scheme;
                    provider.s3.max_retries = config::max_s3_client_retry;
                    provider.s3.retry_slow_down = true;
                    if constexpr (std::is_same_v<Config, AwsConfig>) {
                        provider.sts_request_timeout_ms = config::aws_client_request_timeout_ms;
                    } else if constexpr (std::is_same_v<Config, GcpConfig>) {
                        provider.gcs_signer = io::make_gcs_signed_url_generator;
                    }
                }
            },
            storage_config);
    auto built = build_obj_storage_client(storage_config);
    if (!built.client) {
        return ResultError(
                Status::InvalidArgument("failed to create object storage client: {}", built.error));
    }
    return std::move(built.client);
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
