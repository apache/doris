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

#pragma once

#include <cstdint>
#include <string>

namespace doris {

// This protocol model must remain independent of the Azure SDK: S3 client
// configuration is also used in builds where BUILD_AZURE is disabled.
enum class AzureCredentialType {
    SHARED_KEY,
    SAS,
    OAUTH2,
};

struct AzureCredentialOptions {
    AzureCredentialType type = AzureCredentialType::SHARED_KEY;
    std::string account_name;
    std::string account_key;
    std::string sas_token;
    int64_t sas_expiration_time_ms = 0;
    std::string oauth_client_id;
    std::string oauth_client_secret;
    std::string oauth_tenant_id;
    std::string oauth_server_uri;

    bool operator==(const AzureCredentialOptions&) const = default;

    // Validate the selected credential group, never infer or fall back to
    // another authentication mode. SAS expiry is checked at client access.
    std::string validate() const {
        const bool has_oauth = !oauth_client_id.empty() || !oauth_client_secret.empty() ||
                               !oauth_tenant_id.empty() || !oauth_server_uri.empty();
        switch (type) {
        case AzureCredentialType::SHARED_KEY:
            if (!sas_token.empty() || sas_expiration_time_ms != 0 || has_oauth) {
                return "Azure SharedKey cannot be combined with SAS or OAuth2 credentials";
            }
            // Retain the legacy empty-pair behavior for storage vault callers;
            // the canonical native protocol separately requires both fields.
            if (account_name.empty() != account_key.empty()) {
                return "Azure SharedKey requires both account name and account key";
            }
            break;
        case AzureCredentialType::SAS:
            if (sas_token.empty()) {
                return "Azure SAS credential requires a non-empty token";
            }
            if (!account_key.empty() || has_oauth) {
                return "Azure SAS cannot be combined with SharedKey or OAuth2 credentials";
            }
            if (sas_expiration_time_ms < 0) {
                return "Azure SAS credential has an invalid expiry";
            }
            break;
        case AzureCredentialType::OAUTH2:
            if (!account_key.empty() || !sas_token.empty() || sas_expiration_time_ms != 0) {
                return "Azure OAuth2 cannot be combined with SharedKey or SAS credentials";
            }
            if (oauth_client_id.empty() || oauth_client_secret.empty() ||
                oauth_server_uri.empty()) {
                return "Azure OAuth2 requires client id, client secret, and OAuth server URI";
            }
            break;
        default:
            return "unsupported Azure credential type";
        }
        return {};
    }
};

} // namespace doris
