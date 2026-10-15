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

#include "cpp/aws_common.h"
#include "cpp/obj-client/obj_storage_client.h"

namespace doris::io {

// Client configuration of BE native object storage access, split by backend.
//
// The legacy S3ClientConf carries every provider in one flat struct: Azure reuses
// ak/sk/bucket as account/key/container while region, role_arn and the AWS credential
// provider type stay in the same struct and only mean something for S3. This model
// keeps exactly one backend alternative, so an Azure client cannot depend on an S3-only
// field and an S3 client cannot depend on an Azure-only one. For example, an Azure conf
// whose region differs is still the same Azure client, while for S3 it is a new client.
//
// This header is SDK-free on purpose: it is included by both the S3 and Azure client
// providers and must compile when BUILD_AZURE is off.

struct ObjStorageConnectionOptions {
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

    bool operator==(const S3AuthConfig&) const = default;
};

// S3 and every vendor speaking the S3 protocol (OSS, COS, OBS, BOS, GCP, TOS, ...).
struct S3ClientConfig {
    ObjStorageProvider vendor = ObjStorageProvider::AWS;
    std::string endpoint;
    std::string region;
    // For aws s3, no need to override endpoint.
    bool need_override_endpoint = true;
    bool use_virtual_addressing = true;
    S3AuthConfig auth;

    bool operator==(const S3ClientConfig&) const = default;
};

struct AzureSharedKeyConfig {
    std::string account_key;

    bool operator==(const AzureSharedKeyConfig&) const = default;
};

struct AzureSasConfig {
    // SAS query string without the leading '?'.
    std::string token;
    // Expiry given by the upstream credential source, in Unix milliseconds, when known. The
    // expiry encoded in the token ("se") is checked as well; the earlier one applies.
    std::optional<int64_t> expires_at_ms;

    bool operator==(const AzureSasConfig&) const = default;
};

// Entra ID client credentials flow. The SDK credential requests and refreshes the access
// token itself, from authority_host.
struct AzureClientSecretConfig {
    std::string tenant_id;
    std::string client_id;
    std::string client_secret;
    // Empty means the SDK default authority (Azure public cloud).
    std::string authority_host;

    bool operator==(const AzureClientSecretConfig&) const = default;
};

// One alternative per Azure authentication method, so a SharedKey key and a SAS token can
// never be carried together.
using AzureAuthConfig = std::variant<AzureSharedKeyConfig, AzureSasConfig, AzureClientSecretConfig>;

struct AzureClientConfig {
    std::string endpoint;
    // Required by SharedKey, which signs requests with the account name; may be empty for
    // SAS and client secret, whose requests do not carry it.
    std::string account;
    // The Azure SDK client is bound to one blob container, so the container is part of
    // the client identity. S3 clients are not bound to a bucket and do not carry one.
    std::string container;
    AzureAuthConfig auth;

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

    // Credentials are masked.
    std::string to_string() const;
};

struct ObjClientConfigHash {
    size_t operator()(const ObjClientConfig& config) const {
        return static_cast<size_t>(config.hash());
    }
};

} // namespace doris::io
