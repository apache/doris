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
#include <variant>

#include "cpp/aws_common.h"
#include "cpp/obj-storage/auth/obj_credential.h"
#include "cpp/obj-storage/client/obj_storage_client.h"

namespace doris {

// Fields carried by the FE object-storage properties, storage resource and vault protocols.
struct ObjStorageConfigBase {
    std::string bucket {};
    std::string prefix {};
    bool sse_enabled = false;
    bool is_internal_bucket = false;

    std::string endpoint {};
    std::string region {};
    std::string ak {};
    std::string sk {};
    std::string token {};
    CredentialConfig credential {};
    CredProviderType cred_provider_type = CredProviderType::Default;
    std::string role_arn {};
    std::string external_id {};

    int max_connections = -1;
    int request_timeout_ms = -1;
    int connect_timeout_ms = -1;
    bool use_virtual_addressing = true;
    bool need_override_endpoint = true;

    std::string_view normalized_prefix() const;
    bool operator==(const ObjStorageConfigBase&) const = default;
};

struct AwsConfig : ObjStorageConfigBase {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::AWS;
    bool operator==(const AwsConfig&) const = default;
};

struct OssConfig : ObjStorageConfigBase {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::OSS;
    bool operator==(const OssConfig&) const = default;
};

struct CosConfig : ObjStorageConfigBase {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::COS;
    bool operator==(const CosConfig&) const = default;
};

struct BosConfig : ObjStorageConfigBase {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::BOS;
    bool operator==(const BosConfig&) const = default;
};

struct ObsConfig : ObjStorageConfigBase {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::OBS;
    bool operator==(const ObsConfig&) const = default;
};

struct GcpConfig : ObjStorageConfigBase {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::GCP;
    bool operator==(const GcpConfig&) const = default;
};

struct TosConfig : ObjStorageConfigBase {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::TOS;
    bool operator==(const TosConfig&) const = default;
};

struct AzureConfig : ObjStorageConfigBase {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::AZURE;
    bool operator==(const AzureConfig& other) const;
};

using ObjStorageConfig = std::variant<AwsConfig, OssConfig, CosConfig, BosConfig, ObsConfig,
                                      GcpConfig, TosConfig, AzureConfig>;

const ObjStorageConfigBase& storage_config_base(const ObjStorageConfig& config);
ObjStorageConfigBase& storage_config_base(ObjStorageConfig& config);
ObjStorageProvider storage_provider(const ObjStorageConfig& config);
std::optional<std::string> validate_obj_storage_config(const ObjStorageConfig& config);
bool same_storage_target(const ObjStorageConfig& lhs, const ObjStorageConfig& rhs);
std::string obj_storage_config_to_string(const ObjStorageConfig& config);

// The cache key retains all provider fields and only the location bound to the SDK client.
ObjStorageConfig obj_storage_client_cache_key(const ObjStorageConfig& config);

struct ObjStorageClientHash {
    size_t operator()(const ObjStorageConfig& config) const;
};

std::string hide_access_key(const std::string& ak);

// Unknown endpoints use the S3-compatible client.
cloud::ObjectStoreInfoPB::Provider storage_provider_from_endpoint(std::string_view endpoint);

// BE and Recycler resolve the same persisted vault to the same provider.
cloud::ObjectStoreInfoPB::Provider vault_storage_provider(const cloud::ObjectStoreInfoPB& info);
std::optional<ObjStorageConfig> make_vault_storage_config(
        cloud::ObjectStoreInfoPB::Provider provider);

} // namespace doris
