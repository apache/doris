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

#include "azure_auth_factory.h"

#include <azure/core/datetime.hpp>
#include <azure/core/url.hpp>
#include <azure/identity/client_secret_credential.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <chrono>
#include <optional>
#include <string_view>
#include <utility>

namespace doris {

namespace {

std::string normalize_sas_token(std::string token) {
    while (!token.empty() && (token.front() == '?' || token.front() == '&')) {
        token.erase(token.begin());
    }
    return token;
}

bool contains_line_break(std::string_view value) {
    return value.find('\r') != std::string_view::npos || value.find('\n') != std::string_view::npos;
}

int64_t unix_millis_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
}

std::optional<int64_t> sas_expiry_from_token(std::string_view token, std::string* error) {
    size_t begin = 0;
    while (begin <= token.size()) {
        const auto end = token.find('&', begin);
        const auto field = token.substr(
                begin, end == std::string_view::npos ? token.size() - begin : end - begin);
        if (field.starts_with("se=")) {
            const auto encoded_expiry = field.substr(3);
            if (encoded_expiry.empty()) {
                *error = "Azure SAS credential has an empty expiry";
                return std::nullopt;
            }
            try {
                const auto expiry = Azure::DateTime::Parse(
                        Azure::Core::Url::Decode(std::string(encoded_expiry)),
                        Azure::DateTime::DateFormat::Rfc3339);
                const auto system_time = static_cast<std::chrono::system_clock::time_point>(expiry);
                return std::chrono::duration_cast<std::chrono::milliseconds>(
                               system_time.time_since_epoch())
                        .count();
            } catch (const std::exception&) {
                *error = "Azure SAS credential has an invalid expiry";
                return std::nullopt;
            }
        }
        if (end == std::string_view::npos) {
            break;
        }
        begin = end + 1;
    }
    return std::nullopt;
}

std::string oauth_tenant_from_uri(std::string_view oauth_server_uri) {
    Azure::Core::Url url {std::string(oauth_server_uri)};
    const auto path = Azure::Core::Url::Decode(url.GetPath());
    size_t begin = 0;
    while (begin < path.size() && path[begin] == '/') {
        ++begin;
    }
    while (begin < path.size()) {
        const auto end = path.find('/', begin);
        const auto part = path.substr(begin, end == std::string::npos ? path.size() - begin
                                                                        : end - begin);
        if (!part.empty() && part != "oauth2" && part != "v2.0" && part != "token") {
            return part;
        }
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1;
    }
    return {};
}

std::string oauth_authority_from_uri(std::string_view oauth_server_uri) {
    Azure::Core::Url url {std::string(oauth_server_uri)};
    if (url.GetScheme() != "https" || url.GetHost().empty()) {
        return {};
    }
    std::string authority = url.GetScheme() + "://" + url.GetHost();
    if (url.GetPort() != 0) {
        authority += ":" + std::to_string(url.GetPort());
    }
    return authority;
}

} // namespace

AzureClientBuildResult AzureAuthFactory::create(
        std::string_view container_url, const AzureCredentialOptions& credential,
        Azure::Storage::Blobs::BlobClientOptions client_options) {
    if (credential.type == AzureCredentialType::OAUTH2) {
        if (credential.oauth_client_id.empty() || credential.oauth_client_secret.empty()) {
            return {.error = "Azure OAuth2 credential requires client id and client secret"};
        }
        if (credential.oauth_server_uri.empty()) {
            return {.error = "Azure OAuth2 credential requires an OAuth server URI"};
        }

        try {
            std::string tenant_id = credential.oauth_tenant_id;
            if (tenant_id.empty()) {
                tenant_id = oauth_tenant_from_uri(credential.oauth_server_uri);
            }
            if (tenant_id.empty()) {
                return {.error = "Azure OAuth2 credential requires a tenant id"};
            }
            Azure::Identity::ClientSecretCredentialOptions identity_options;
            identity_options.AuthorityHost =
                    oauth_authority_from_uri(credential.oauth_server_uri);
            if (identity_options.AuthorityHost.empty()) {
                return {.error = "Azure OAuth2 credential has an invalid OAuth server URI"};
            }
            auto token_credential = std::make_shared<Azure::Identity::ClientSecretCredential>(
                    std::move(tenant_id), credential.oauth_client_id,
                    credential.oauth_client_secret, std::move(identity_options));
            std::shared_ptr<const Azure::Core::Credentials::TokenCredential> credential_view =
                    std::move(token_credential);
            auto client = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
                    std::string(container_url), std::move(credential_view),
                    std::move(client_options));
            return {.container_client = std::move(client)};
        } catch (const std::exception& e) {
            return {.error = std::string("failed to create Azure OAuth2 credential: ") + e.what()};
        }
    }

    if (credential.type == AzureCredentialType::SAS) {
        auto token = normalize_sas_token(credential.sas_token);
        if (token.empty()) {
            return {.error = "Azure SAS credential requires a non-empty token"};
        }
        if (contains_line_break(token)) {
            return {.error = "Azure SAS token contains a line break"};
        }
        if (credential.sas_expiration_time_ms < 0) {
            return {.error = "Azure SAS credential has an invalid expiry"};
        }
        if (credential.sas_expiration_time_ms > 0 &&
            credential.sas_expiration_time_ms <= unix_millis_now()) {
            return {.error = "Azure SAS credential is expired"};
        }
        std::string expiry_error;
        if (const auto token_expiry = sas_expiry_from_token(token, &expiry_error);
            !expiry_error.empty()) {
            return {.error = std::move(expiry_error)};
        } else if (token_expiry.has_value() && *token_expiry <= unix_millis_now()) {
            return {.error = "Azure SAS credential is expired"};
        }

        std::string sas_url(container_url);
        sas_url += sas_url.find('?') == std::string::npos ? '?' : '&';
        sas_url += token;
        auto client = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
                std::move(sas_url), std::move(client_options));
        return {.container_client = std::move(client)};
    }

    if (credential.type != AzureCredentialType::SHARED_KEY) {
        return {.error = "unsupported Azure credential type"};
    }

    auto shared_key = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
            credential.account_name, credential.account_key);
    auto client = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
            std::string(container_url), shared_key, std::move(client_options));
    return {
            .container_client = std::move(client),
            .shared_key_credential = std::move(shared_key),
    };
}

} // namespace doris
