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

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "cpp/obj-storage/client/obj_storage_client.h"

namespace doris {

struct ObjStorageConnectionConfig {
    std::string endpoint;
    std::optional<std::string> ca_cert_file;
    std::optional<int> max_connections;
    std::optional<std::chrono::milliseconds> connect_timeout;
    std::optional<std::chrono::milliseconds> request_timeout;
    std::optional<int> max_retries;
    bool operator==(const ObjStorageConnectionConfig&) const = default;
};

struct AnonymousObjStorageAuth {
    bool operator==(const AnonymousObjStorageAuth&) const = default;
};

struct AccessKeyObjStorageAuth {
    std::string access_key;
    std::string secret_key;
    std::string session_token;
    bool operator==(const AccessKeyObjStorageAuth&) const = default;
};

enum class AwsCredentialChainType {
    Default,
    Environment,
    Profile,
    WebIdentity,
    Container,
    InstanceProfile
};
enum class AwsCredentialImplementation { V1, V2 };

using AwsCredentialSource =
        std::variant<AnonymousObjStorageAuth, AccessKeyObjStorageAuth, AwsCredentialChainType>;

struct AwsAssumeRole {
    std::string role_arn;
    std::optional<std::string> external_id;
    std::optional<std::chrono::milliseconds> request_timeout;
    bool operator==(const AwsAssumeRole&) const = default;
};

struct AwsObjStorageAuthConfig {
    AwsCredentialSource source;
    std::optional<AwsAssumeRole> assume_role;
    AwsCredentialImplementation implementation = AwsCredentialImplementation::V1;
    bool operator==(const AwsObjStorageAuthConfig&) const = default;
};

// Previously persisted S3-compatible resources can use SDK credentials, including
// STS. Temporary access keys and session tokens obtained by the FE use AccessKeyObjStorageAuth.
struct S3CredentialChain {
    std::variant<AnonymousObjStorageAuth, AwsCredentialChainType> source;
    AwsCredentialImplementation implementation = AwsCredentialImplementation::V1;
    std::optional<AwsAssumeRole> assume_role;
    bool operator==(const S3CredentialChain&) const = default;
};

using S3CompatibleObjStorageAuth =
        std::variant<AnonymousObjStorageAuth, AccessKeyObjStorageAuth, S3CredentialChain>;

struct GcpHmacObjStorageAuth {
    std::string access_id;
    std::string secret_key;
    std::string session_token;
    bool operator==(const GcpHmacObjStorageAuth&) const = default;
};

enum class GcpCredentialSource { Default, ComputeEngine };
struct GcpNativeObjStorageAuth {
    GcpCredentialSource source = GcpCredentialSource::Default;
    std::optional<std::string> impersonation_service_account;
    bool operator==(const GcpNativeObjStorageAuth&) const = default;
};
using GcpObjStorageAuthConfig = std::variant<AnonymousObjStorageAuth, GcpHmacObjStorageAuth,
                                             GcpNativeObjStorageAuth, S3CredentialChain>;

struct AzureObjStorageAuthConfig {
    std::string account_name;
    std::string account_key;
    bool operator==(const AzureObjStorageAuthConfig&) const = default;
};

struct AwsObjStorageConfig {
    static constexpr auto provider = ObjStorageProvider::AWS;
    ObjStorageConnectionConfig connection;
    AwsObjStorageAuthConfig auth;
    std::string region;
    bool use_path_style = false;
    bool use_sdk_endpoint = false;
    bool sdk_endpoint_http = false;
    bool retry_slow_down = false;
    std::optional<std::string> validate() const;
    bool operator==(const AwsObjStorageConfig&) const = default;
};

struct OssObjStorageConfig {
    static constexpr auto provider = ObjStorageProvider::OSS;
    ObjStorageConnectionConfig connection;
    S3CompatibleObjStorageAuth auth;
    std::string region;
    bool use_path_style = false;
    bool retry_slow_down = false;
    std::optional<std::string> validate() const;
    bool operator==(const OssObjStorageConfig&) const = default;
};

struct CosObjStorageConfig {
    static constexpr auto provider = ObjStorageProvider::COS;
    ObjStorageConnectionConfig connection;
    S3CompatibleObjStorageAuth auth;
    std::string region;
    bool use_path_style = false;
    bool retry_slow_down = false;
    std::optional<std::string> validate() const;
    bool operator==(const CosObjStorageConfig&) const = default;
};

struct BosObjStorageConfig {
    static constexpr auto provider = ObjStorageProvider::BOS;
    ObjStorageConnectionConfig connection;
    S3CompatibleObjStorageAuth auth;
    std::string region;
    bool use_path_style = false;
    bool retry_slow_down = false;
    std::optional<std::string> validate() const;
    bool operator==(const BosObjStorageConfig&) const = default;
};

struct ObsObjStorageConfig {
    static constexpr auto provider = ObjStorageProvider::OBS;
    ObjStorageConnectionConfig connection;
    S3CompatibleObjStorageAuth auth;
    std::string region;
    bool use_path_style = false;
    bool retry_slow_down = false;
    std::optional<std::string> validate() const;
    bool operator==(const ObsObjStorageConfig&) const = default;
};

struct TosObjStorageConfig {
    static constexpr auto provider = ObjStorageProvider::TOS;
    ObjStorageConnectionConfig connection;
    S3CompatibleObjStorageAuth auth;
    std::string region;
    bool use_path_style = false;
    bool retry_slow_down = false;
    std::optional<std::string> validate() const;
    bool operator==(const TosObjStorageConfig&) const = default;
};

struct GcpObjStorageConfig {
    static constexpr auto provider = ObjStorageProvider::GCP;
    ObjStorageConnectionConfig connection;
    GcpObjStorageAuthConfig auth;
    std::string region;
    bool use_path_style = false;
    bool retry_slow_down = false;
    std::optional<std::string> validate() const;
    bool operator==(const GcpObjStorageConfig&) const = default;
};

struct AzureObjStorageConfig {
    static constexpr auto provider = ObjStorageProvider::AZURE;
    ObjStorageConnectionConfig connection;
    AzureObjStorageAuthConfig auth;
    bool retry_too_many_requests = false;
    std::optional<std::string> validate() const;
    bool operator==(const AzureObjStorageConfig&) const = default;
};

using ObjStorageClientConfig =
        std::variant<AwsObjStorageConfig, OssObjStorageConfig, CosObjStorageConfig,
                     BosObjStorageConfig, ObsObjStorageConfig, TosObjStorageConfig,
                     GcpObjStorageConfig, AzureObjStorageConfig>;

struct ObjStorageResourceConfig {
    std::string bucket;
    std::string prefix;
    ObjStorageClientConfig client;
    bool is_internal_bucket = false;
    bool sse_enabled = false;
    std::string_view normalized_prefix() const;
};

const ObjStorageConnectionConfig& obj_storage_connection(const ObjStorageClientConfig& config);
ObjStorageConnectionConfig& obj_storage_connection(ObjStorageClientConfig& config);
ObjStorageProvider storage_provider(const ObjStorageClientConfig& config);
std::optional<std::string> validate_obj_storage_config(const ObjStorageClientConfig& config);
bool same_client_target(const ObjStorageClientConfig& lhs, const ObjStorageClientConfig& rhs);
std::string obj_storage_config_to_string(const ObjStorageClientConfig& config);

struct ObjStorageClientHash {
    size_t operator()(const ObjStorageClientConfig& config) const;
};

std::string hide_access_key(const std::string& ak);

} // namespace doris
