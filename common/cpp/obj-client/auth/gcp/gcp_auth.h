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

#include <optional>
#include <string>
#include <string_view>

#include "cpp/obj-client/auth/obj_credential_common.h"

namespace doris {

// Provider-specific authentication lives under cpp/obj-client/auth/<provider>. Future
// providers should use sibling packages and expose only their normalized config,
// validation, and client-setup entry points there.

inline constexpr std::string_view GCP_CREDENTIAL_PROVIDER_TYPE = "gs.credential_provider_type";
inline constexpr std::string_view GCP_IMPERSONATION_SERVICE_ACCOUNT =
        "gs.impersonation_service_account";

enum class GcpCredentialProviderType {
    Default,
    ComputeEngine,
};

struct GcpCredentialConfig {
    GcpCredentialProviderType provider_type = GcpCredentialProviderType::Default;
    std::string impersonation_service_account {};

    bool operator==(const GcpCredentialConfig&) const = default;
};

struct GcpCredentialParseResult {
    std::optional<GcpCredentialConfig> credential = std::nullopt;
    std::optional<std::string> error = std::nullopt;
};

bool is_valid_gcp_service_account_email(std::string_view email);
std::optional<GcpCredentialProviderType> parse_gcp_credential_provider_type(
        std::string_view provider_type);
std::optional<std::string> validate_gcp_credential(const GcpCredentialConfig& credential);
GcpCredentialParseResult parse_gcp_credential_properties(
        const ObjCredentialPropertyGetter& get_property);

} // namespace doris
