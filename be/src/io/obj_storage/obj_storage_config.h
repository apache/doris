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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>

#include "common/status.h"
#include "cpp/aws_common.h"
#include "cpp/obj-client/auth/azure/azure_auth.h"
#include "cpp/obj-client/auth/obj_credential.h"
#include "cpp/obj-client/obj_storage_client.h"

namespace doris::io {

// Client configuration of BE native object storage access, split by backend.
//
// The flat parameters FE and the storage metadata send (AWS_* properties, TS3StorageParam,
// ObjectStoreInfoPB) also use S3 fields for Azure. Keep those inputs' validation, cache
// identity and refresh rules while giving each provider its own SDK parameters.
//
// This header is SDK-free on purpose: it is included by both the S3 and Azure client
// providers and must compile when BUILD_AZURE is off.

// Only S3 clients apply these options today. Azure clients ignore them, although they still
// tell cached Azure clients apart, as in the legacy conf.
struct ObjStorageConnectionOptions {
    // Maximum number of connections per cached S3 client on one BE.
    // Non-positive values keep the SDK/BE defaults, as in the legacy conf.
    int max_connections = -1;
    int request_timeout_ms = -1;
    int connect_timeout_ms = -1;

    bool operator==(const ObjStorageConnectionOptions&) const = default;
};

// Who the client serves. It selects the shared BE rate-limit policy, so two otherwise
// identical configurations with different usages must never share a cached client.
enum class ClientUsage : uint8_t {
    // A Doris internal bucket, e.g. a storage vault in cloud mode.
    INTERNAL_STORAGE,
    // An external bucket, e.g. S3 load, TVF, external catalogs or export.
    EXTERNAL_ACCESS,
};

struct S3AuthConfig {
    std::string access_key;
    std::string secret_key;
    std::string session_token;
    CredProviderType provider_type = CredProviderType::Default;
    std::string role_arn;
    std::string external_id;
    // Provider-native passwordless credential, e.g. GCP IAM; monostate for static keys and the
    // AWS credential provider chain.
    CredentialConfig credential {};

    bool operator==(const S3AuthConfig&) const = default;
};

// S3 and every vendor speaking the S3 protocol (OSS, COS, OBS, BOS, GCP, TOS, ...).
struct S3ClientConfig {
    ObjStorageProvider vendor = ObjStorageProvider::AWS;
    std::string endpoint;
    std::string region;
    // The BE cache distinguishes buckets. The SDK takes the bucket from each request.
    std::string cache_bucket;
    // For aws s3, no need to override endpoint.
    bool need_override_endpoint = true;
    bool use_virtual_addressing = true;
    S3AuthConfig auth;

    bool operator==(const S3ClientConfig&) const = default;
};

// Fields carried by the historical S3-shaped Azure inputs. The Azure SDK ignores them,
// but the BE uses them for validation, client cache identity and resource/vault refresh.
struct LegacyAzureClientFields {
    std::string region;
    bool need_override_endpoint = true;
    bool use_virtual_addressing = true;
    std::string session_token;
    CredProviderType provider_type = CredProviderType::Default;
    std::string role_arn;
    std::string external_id;
    CredentialConfig credential;

    bool operator==(const LegacyAzureClientFields&) const = default;
};

// The authentication is the AzureAuthConfig shared with Recycler (common/cpp,
// azure/azure_auth.h), which owns its methods and their rules; BE only adds the storage target.
struct AzureClientConfig {
    std::string endpoint;
    // The storage account the endpoint names; SharedKey signs requests with it.
    std::string account;
    // The Azure SDK client is bound to one blob container.
    std::string container;
    AzureAuthConfig auth;
    // Only the three flat inputs populate these fields. Native Azure has its own rules.
    std::optional<LegacyAzureClientFields> legacy = std::nullopt;

    bool operator==(const AzureClientConfig&) const = default;
};

struct ObjClientConfig {
    ObjStorageConnectionOptions connection;
    ClientUsage usage = ClientUsage::EXTERNAL_ACCESS;
    // The alternative decides which client provider builds the client; there is no
    // separate provider field that could disagree with it.
    std::variant<S3ClientConfig, AzureClientConfig> backend;

    // Full-field identity. hash() only picks a cache bucket; distinct configurations can
    // collide, so hash equality must never be treated as configuration equality.
    bool operator==(const ObjClientConfig&) const = default;

    uint64_t hash() const;

    ObjStorageProvider provider() const;

    const std::string& endpoint() const;
    bool has_role_arn() const;

    // Credentials are masked.
    std::string to_string() const;
};

// One object storage binding: the bucket (Azure: container) and root prefix it serves, plus
// the client configuration. Every input source (FE properties, TS3StorageParam,
// ObjectStoreInfoPB) is turned into this by ObjStorageConfigResolver.
struct ObjStorageConfig {
    std::string bucket;
    std::string prefix;
    ObjClientConfig client;
    // Vault metadata reported in BE diagnostics.
    bool sse_enabled = false;

    // Credentials are masked.
    std::string to_string() const;
};

// Whether the configuration is valid for its backend. Every client request is checked with it
// before anything is built or looked up (ObjClientFactory), and FE properties are checked with
// it right after conversion (ObjStorageConfigResolver).
Status validate_obj_client_config(const ObjClientConfig& config);

// Resource and vault refresh keep the original endpoint, region and endpoint override,
// including across a change of backend. The remaining fields come from the update.
ObjClientConfig merge_obj_client_config(const ObjClientConfig& current, ObjClientConfig update);

struct ObjClientConfigHash {
    size_t operator()(const ObjClientConfig& config) const {
        return static_cast<size_t>(config.hash());
    }
};

} // namespace doris::io
