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

#include "io/obj_storage/azure_client_provider.h"

#include <fmt/format.h>

#include <utility>

#include "common/logging.h"
#include "io/obj_storage/obj_client_factory.h"

#ifdef USE_AZURE
#include <azure/core/http/curl_transport.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <variant>

#include "common/config.h"
#include "cpp/obj-client/auth/azure_auth_factory.h"
#include "cpp/obj-client/azure_obj_storage_client.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/util.h"
#include "util/s3_util.h"
#endif

namespace doris::io {
namespace {

#ifdef USE_AZURE
std::string azure_env_or_empty(const char* env_name) {
    if (const char* value = std::getenv(env_name); value != nullptr) {
        return value;
    }
    return "";
}

std::string build_azure_tls_debug_context(const std::string& selected_ca_file) {
    bool selected_ca_exists = false;
    bool selected_ca_readable = false;
    if (!selected_ca_file.empty()) {
        std::error_code ec;
        selected_ca_exists = std::filesystem::exists(selected_ca_file, ec) && !ec;
        std::ifstream input(selected_ca_file);
        selected_ca_readable = input.good();
    }

    return fmt::format(
            "tls_debug(ca_cert_file_paths='{}', selected_ca_file='{}', selected_ca_exists={}, "
            "selected_ca_readable={}, SSL_CERT_FILE='{}', CURL_CA_BUNDLE='{}', SSL_CERT_DIR='{}')",
            config::ca_cert_file_paths, selected_ca_file, selected_ca_exists, selected_ca_readable,
            azure_env_or_empty("SSL_CERT_FILE"), azure_env_or_empty("CURL_CA_BUNDLE"),
            azure_env_or_empty("SSL_CERT_DIR"));
}

AzureCredentialOptions to_credential_options(const std::string& account,
                                             const AzureSharedKeyConfig& shared_key) {
    return {
            .type = AzureCredentialType::SHARED_KEY,
            .account_name = account,
            .account_key = shared_key.account_key,
    };
}

// The SharedKey pair is also reported as ak/sk endpoint info, as before the split.
std::string account_key_for_endpoint_info(const AzureSharedKeyConfig& shared_key) {
    return shared_key.account_key;
}
#endif

} // namespace

Result<std::shared_ptr<ObjStorageClient>> AzureClientProvider::get_or_create(
        const ObjClientConfig& config, const AzureClientConfig& azure) {
    if (auto cached = _cache.find(config)) {
        return cached;
    }

    auto client_result = _build(azure);
    if (!client_result.has_value()) {
        return ResultError(std::move(client_result).error());
    }
    auto obj_client = apply_rate_limit_policy(std::move(client_result).value(), config.usage);
    return _cache.publish(config, obj_client);
}

Result<std::shared_ptr<ObjStorageClient>> AzureClientProvider::_build(
        const AzureClientConfig& azure) {
#ifdef USE_AZURE
    std::string uri = fmt::format("{}/{}", azure.endpoint, azure.container);
    if (azure.endpoint.find("://") == std::string::npos) {
        uri = "https://" + uri;
    }

    Azure::Storage::Blobs::BlobClientOptions options;
    options.Retry.StatusCodes.insert(Azure::Core::Http::HttpStatusCode::TooManyRequests);
    options.Retry.MaxRetries = config::max_s3_client_retry;
    options.PerRetryPolicies.emplace_back(std::make_unique<AzureRetryRecordPolicy>());
    auto ca_cert_file_path = S3ClientFactory::instance().ca_cert_file_path();
    if (!ca_cert_file_path.empty()) {
        Azure::Core::Http::CurlTransportOptions curl_options;
        curl_options.CAInfo = ca_cert_file_path;
        options.Transport.Transport =
                std::make_shared<Azure::Core::Http::CurlTransport>(std::move(curl_options));
    }

    std::string normalized_uri = normalize_http_uri(uri);
    VLOG_DEBUG << "uri:" << uri << ", normalized_uri:" << normalized_uri;
    std::string tls_debug_context = build_azure_tls_debug_context(ca_cert_file_path);

    auto credential =
            std::visit([&](const auto& auth) { return to_credential_options(azure.account, auth); },
                       azure.auth);
    auto built = AzureAuthFactory::create(normalized_uri, credential, std::move(options));
    if (!built) {
        return ResultError(
                Status::InvalidArgument("failed to create Azure client: {}", built.error));
    }
    LOG_INFO("create one azure client with endpoint={}, account={}, container={}", azure.endpoint,
             azure.account, azure.container);
    return std::make_shared<AzureObjStorageClient>(
            std::move(built.container_client),
            ObjStorageEndpointInfo {
                    .endpoint = azure.endpoint,
                    .ak = azure.account,
                    .sk = std::visit(
                            [](const auto& auth) { return account_key_for_endpoint_info(auth); },
                            azure.auth),
                    .tls_debug_context = std::move(tls_debug_context),
            },
            std::move(built.shared_key_credential));
#else
    return ResultError(Status::NotSupported(
            "BE is not compiled with azure support, export BUILD_AZURE=ON before building"));
#endif
}

} // namespace doris::io
