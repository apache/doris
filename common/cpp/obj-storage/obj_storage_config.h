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

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "cpp/aws_common.h"
#include "cpp/obj-storage/auth/gcp/gcp_auth.h"
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
    std::string_view normalized_prefix() const;
    bool operator==(const ObjStorageConfigBase&) const = default;
};

// SDK defaults supplied by the process when it opens an S3-compatible connection.
struct S3ClientSettings {
    std::string credential_provider_version {};
    EmptyCredentialsBehavior empty_credentials = EmptyCredentialsBehavior::DEFAULT_CHAIN;
    std::string ca_cert_path {};
    std::string http_scheme {};
    int max_retries = 0;
    bool retry_slow_down = false;
    std::optional<int64_t> min_connections {};
    bool operator==(const S3ClientSettings&) const = default;
};

struct S3CompatibleConfig : ObjStorageConfigBase {
    std::string token {};
    // FE sends this AWS SDK credential mode for every S3-compatible provider.
    CredProviderType cred_provider_type = CredProviderType::Default;
    int max_connections = -1;
    int request_timeout_ms = -1;
    int connect_timeout_ms = -1;
    bool use_virtual_addressing = true;
    bool need_override_endpoint = true;
    S3ClientSettings s3 {};
    bool operator==(const S3CompatibleConfig&) const = default;
};

using ObjStorageSignedUrlGenerator = std::function<std::string(const ObjStoragePath&, int64_t)>;

struct AwsConfig : S3CompatibleConfig {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::AWS;
    std::optional<long> sts_request_timeout_ms {};
    std::string role_arn {};
    std::string external_id {};
    bool operator==(const AwsConfig&) const = default;
};

struct OssConfig : S3CompatibleConfig {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::OSS;
    bool operator==(const OssConfig&) const = default;
};

struct CosConfig : S3CompatibleConfig {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::COS;
    bool operator==(const CosConfig&) const = default;
};

struct BosConfig : S3CompatibleConfig {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::BOS;
    bool operator==(const BosConfig&) const = default;
};

struct ObsConfig : S3CompatibleConfig {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::OBS;
    bool operator==(const ObsConfig&) const = default;
};

struct GcpConfig : S3CompatibleConfig {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::GCP;
    std::optional<GcpCredentialConfig> credential {};
    ObjStorageSignedUrlGenerator (*gcs_signer)(const GcpCredentialConfig&, std::string, int,
                                               std::string) = nullptr;
    std::optional<std::string> validate_credential(std::string_view aws_role_arn = {},
                                                   std::string_view aws_external_id = {}) const;
    bool operator==(const GcpConfig&) const = default;
};

struct TosConfig : S3CompatibleConfig {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::TOS;
    bool operator==(const TosConfig&) const = default;
};

struct AzureConfig : ObjStorageConfigBase {
    static constexpr ObjStorageProvider provider = ObjStorageProvider::AZURE;
    int max_retries = 0;
    int azure_log_level = 4;
    bool retry_too_many_requests = false;
    std::string ca_cert_path {};
    std::optional<std::string> ca_cert_file_paths {};
    bool operator==(const AzureConfig&) const = default;
};

using ObjStorageConfig = std::variant<AwsConfig, OssConfig, CosConfig, BosConfig, ObsConfig,
                                      GcpConfig, TosConfig, AzureConfig>;

ObjStorageClientBuildResult build_obj_storage_client(const ObjStorageConfig& config);
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
std::optional<std::string> parse_vault_storage_config(const cloud::ObjectStoreInfoPB& info,
                                                      ObjStorageConfig* result);

// Invoke the input parser with the selected provider's concrete configuration.
template <typename Parser>
std::optional<std::string> parse_storage_provider(cloud::ObjectStoreInfoPB::Provider provider,
                                                  Parser parse) {
    using Vault = cloud::ObjectStoreInfoPB;
    switch (provider) {
    case Vault::S3:
        return parse(AwsConfig {});
    case Vault::OSS:
        return parse(OssConfig {});
    case Vault::COS:
        return parse(CosConfig {});
    case Vault::BOS:
        return parse(BosConfig {});
    case Vault::OBS:
        return parse(ObsConfig {});
    case Vault::GCP:
        return parse(GcpConfig {});
    case Vault::TOS:
        return parse(TosConfig {});
    case Vault::AZURE:
        return parse(AzureConfig {});
    default:
        return "Unknown object storage provider " + std::to_string(provider);
    }
}

} // namespace doris
