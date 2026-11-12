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
    return std::nullopt;
}

std::optional<std::string> validate_gcp_credential(const GcpCredentialConfig& credential) {
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
    if (source == properties.end() && target == properties.end()) {
        return {};
    }
    GcpCredentialConfig config;
    if (source != properties.end()) {
        auto mode = parse_gcp_credential_provider_type(source->second);
        if (!mode) {
            return {.error = "invalid " + std::string(GCP_CREDENTIAL_PROVIDER_TYPE) + " value \"" +
                             source->second +
                             "\"; supported values are DEFAULT and COMPUTE_ENGINE"};
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
