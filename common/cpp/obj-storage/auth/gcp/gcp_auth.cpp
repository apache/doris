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

#include "cpp/obj-storage/auth/gcp/gcp_auth.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <regex>
#include <utility>

namespace doris {

bool is_valid_gcp_storage_endpoint(std::string_view endpoint) {
    // Only service-base XML API endpoints: the S3 clients add the bucket themselves.
    static const std::regex pattern(
            R"(https://(storage\.googleapis\.com|[a-z0-9-]+-storage\.googleapis\.com|storage\.[a-z0-9-]+\.rep\.googleapis\.com)(:443)?/?)",
            std::regex::icase);
    return std::regex_match(endpoint.begin(), endpoint.end(), pattern);
}

bool is_valid_gcp_service_account_email(std::string_view email) {
    constexpr std::string_view user_managed_suffix = ".iam.gserviceaccount.com";
    constexpr std::string_view compute_engine_domain = "developer.gserviceaccount.com";
    constexpr std::string_view app_engine_domain = "appspot.gserviceaccount.com";
    auto at = email.find('@');
    if (at == std::string_view::npos || at == 0 ||
        email.find('@', at + 1) != std::string_view::npos ||
        std::any_of(email.begin(), email.end(), [](unsigned char c) { return std::isspace(c); })) {
        return false;
    }

    auto domain = email.substr(at + 1);
    return domain == compute_engine_domain || domain == app_engine_domain ||
           (domain.size() > user_managed_suffix.size() && domain.ends_with(user_managed_suffix));
}

std::optional<GcpCredentialProviderType> parse_gcp_credential_provider_type(
        std::string_view provider_type) {
    std::string normalized(provider_type);
    normalized.erase(normalized.begin(),
                     std::find_if(normalized.begin(), normalized.end(),
                                  [](unsigned char c) { return !std::isspace(c); }));
    normalized.erase(std::find_if(normalized.rbegin(), normalized.rend(),
                                  [](unsigned char c) { return !std::isspace(c); })
                             .base(),
                     normalized.end());
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    if (normalized == "DEFAULT") {
        return GcpCredentialProviderType::Default;
    }
    if (normalized == "COMPUTE_ENGINE") {
        return GcpCredentialProviderType::ComputeEngine;
    }
    if (normalized == "ACCESS_TOKEN") {
        return GcpCredentialProviderType::AccessToken;
    }
    return std::nullopt;
}

std::optional<std::string> validate_gcp_credential(const GcpCredentialConfig& credential) {
    if (credential.provider_type == GcpCredentialProviderType::AccessToken) {
        if (credential.access_token.empty() ||
            std::any_of(credential.access_token.begin(), credential.access_token.end(),
                        [](unsigned char c) { return std::isspace(c); })) {
            return "GCS access token must be nonempty and contain no whitespace";
        }
        if (!credential.impersonation_service_account.empty()) {
            return "GCS access token cannot be combined with impersonation";
        }
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
        if (credential.expires_at_ms < 0 ||
            (credential.expires_at_ms > 0 && credential.expires_at_ms <= now_ms)) {
            return "GCS access token has expired; reload catalog credentials";
        }
    } else if (!credential.access_token.empty() || credential.expires_at_ms != 0) {
        return "GCS access token requires ACCESS_TOKEN authentication";
    }
    if (credential.impersonation_service_account.empty()) {
        return std::nullopt;
    }
    if (!is_valid_gcp_service_account_email(credential.impersonation_service_account)) {
        return "Invalid GCP service account email";
    }
    return std::nullopt;
}

GcpCredentialParseResult parse_gcp_credential_properties(
        const std::map<std::string, std::string>& properties) {
    auto source = properties.find(std::string(GCP_CREDENTIAL_PROVIDER_TYPE));
    auto target = properties.find(std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT));
    auto token = properties.find(std::string(GCP_OAUTH2_TOKEN));
    auto expiration = properties.find(std::string(GCP_OAUTH2_TOKEN_EXPIRES_AT));
    if (source == properties.end() && target == properties.end() && token == properties.end() &&
        expiration == properties.end()) {
        return {};
    }
    GcpCredentialConfig config;
    if (token != properties.end()) {
        config.provider_type = GcpCredentialProviderType::AccessToken;
        config.access_token = token->second;
    }
    if (expiration != properties.end()) {
        const auto& value = expiration->second;
        auto [end, error] =
                std::from_chars(value.data(), value.data() + value.size(), config.expires_at_ms);
        if (error != std::errc() || end != value.data() + value.size() ||
            config.expires_at_ms <= 0) {
            return {.error = "GCS access token expiration must be positive epoch milliseconds"};
        }
    }
    if (source != properties.end()) {
        auto mode = parse_gcp_credential_provider_type(source->second);
        if (!mode) {
            return {.error = "invalid " + std::string(GCP_CREDENTIAL_PROVIDER_TYPE) + " value \"" +
                             source->second +
                             "\"; supported values are DEFAULT, COMPUTE_ENGINE and ACCESS_TOKEN"};
        }
        config.provider_type = *mode;
    }
    if (target != properties.end()) {
        config.impersonation_service_account = target->second;
    }
    if (auto error = validate_gcp_credential(config)) {
        return {.error = std::move(error)};
    }
    return {.credential = std::move(config)};
}

} // namespace doris
