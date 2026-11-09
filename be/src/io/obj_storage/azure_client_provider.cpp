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

#include <mutex>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "io/obj_storage/obj_client_rate_limit.h"
#include "util/time.h"

#ifdef USE_AZURE
#include <azure/core/http/curl_transport.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include "cpp/obj-client/auth/azure/azure_auth_factory.h"
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
#endif

// An expired SAS, or one whose expiry cannot be read, is rejected before any request is sent.
Status check_azure_auth_expiry(const AzureAuthConfig& auth, int64_t now_ms) {
    if (auto error = validate_azure_auth_expiry(auth, now_ms); error.has_value()) {
        return Status::InvalidArgument<false>(*error);
    }
    return Status::OK();
}

// The cache's initial capacity, used until set_cache_capacity() gives it one. set_config writes
// a new value before validating it and restores the old one when it is invalid, all under this
// lock; reading under it never sees that transient value, e.g. a rejected 0 that ObjClientCache
// would treat as a broken invariant.
size_t read_azure_client_cache_capacity() {
    std::lock_guard lock(*config::get_mutable_string_config_lock());
    return static_cast<size_t>(config::azure_client_cache_capacity);
}

} // namespace

Result<std::shared_ptr<ObjStorageClient>> AzureClientProvider::get_or_create(
        const ObjClientConfig& config, const AzureClientConfig& azure) {
    // Checked before the cache lookup so that a cached client never outlives its SAS: every
    // reader or writer open passes through here, while range reads reuse the opened client.
    RETURN_IF_ERROR_RESULT(check_azure_auth_expiry(azure.auth, UnixMillis()));
    if (auto cached = _cache.find(config)) {
        return cached;
    }

    auto client_result = _build(azure);
    if (!client_result.has_value()) {
        return ResultError(std::move(client_result).error());
    }
    auto obj_client = apply_rate_limit_policy(std::move(client_result).value(), config.usage);
    // Building can outlast a short-lived SAS; an expired client must not enter the cache.
    int64_t now_ms = UnixMillis();
    RETURN_IF_ERROR_RESULT(check_azure_auth_expiry(azure.auth, now_ms));
    return _cache.publish(config, obj_client, azure_auth_expiry(azure.auth).expires_at_ms, now_ms,
                          read_azure_client_cache_capacity());
}

void AzureClientProvider::set_cache_capacity(size_t capacity) {
    _cache.set_capacity(UnixMillis(), capacity);
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
    auto ca_cert_file_path = AwsSdkManager::instance().ca_cert_file_path();
    if (!ca_cert_file_path.empty()) {
        Azure::Core::Http::CurlTransportOptions curl_options;
        curl_options.CAInfo = ca_cert_file_path;
        options.Transport.Transport =
                std::make_shared<Azure::Core::Http::CurlTransport>(std::move(curl_options));
    }

    std::string normalized_uri = normalize_http_uri(uri);
    VLOG_DEBUG << "uri:" << uri << ", normalized_uri:" << normalized_uri;
    std::string tls_debug_context = build_azure_tls_debug_context(ca_cert_file_path);

    auto built =
            AzureAuthFactory::create(normalized_uri, azure.account, azure.auth, std::move(options));
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
                    .sk = azure_auth_account_key(azure.auth),
                    .tls_debug_context = std::move(tls_debug_context),
            },
            std::move(built.shared_key_credential));
#else
    return ResultError(Status::NotSupported(
            "BE is not compiled with azure support, export BUILD_AZURE=ON before building"));
#endif
}

} // namespace doris::io
