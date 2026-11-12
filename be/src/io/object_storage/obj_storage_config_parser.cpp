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

#include "io/object_storage/obj_storage_config_parser.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/cloud.pb.h>

#include <algorithm>
#include <type_traits>

#include "common/config.h"
#include "cpp/obj-storage/auth/gcp/gcp_auth.h"
#include "cpp/obj-storage/obj_storage_config_parser.h"
#include "io/object_storage/aws_client_settings.h"
#include "util/s3_uri.h"
#include "util/string_util.h"

namespace doris {
namespace {
using Vault = cloud::ObjectStoreInfoPB;
using Properties = std::map<std::string, std::string>;
struct ProviderMapping {
    std::string_view name;
    TObjStorageType::type resource;
    cloud::ObjectStoreInfoPB::Provider vault;
};

constexpr ProviderMapping PROVIDERS[] = {
        {"AWS", TObjStorageType::AWS, cloud::ObjectStoreInfoPB::S3},
        {"S3", TObjStorageType::AWS, cloud::ObjectStoreInfoPB::S3},
        {"OSS", TObjStorageType::OSS, cloud::ObjectStoreInfoPB::OSS},
        {"COS", TObjStorageType::COS, cloud::ObjectStoreInfoPB::COS},
        {"BOS", TObjStorageType::BOS, cloud::ObjectStoreInfoPB::BOS},
        {"OBS", TObjStorageType::OBS, cloud::ObjectStoreInfoPB::OBS},
        {"GCP", TObjStorageType::GCP, cloud::ObjectStoreInfoPB::GCP},
        {"GCS", TObjStorageType::GCP, cloud::ObjectStoreInfoPB::GCP},
        {"TOS", TObjStorageType::TOS, cloud::ObjectStoreInfoPB::TOS},
        {"AZURE", TObjStorageType::AZURE, cloud::ObjectStoreInfoPB::AZURE},
};

Result<ObjStorageResourceConfig> parse_resource(const Properties& properties,
                                                Vault::Provider provider, std::string bucket,
                                                std::string prefix) {
    ObjStorageResourceConfig parsed {.bucket = std::move(bucket), .prefix = std::move(prefix)};
    if (auto e = parse_obj_storage_client_config(
                provider, properties,
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS, &parsed.client)) {
        return ResultError(Status::InvalidArgument<false>(*e));
    }
    if (parsed.bucket.empty()) {
        return ResultError(
                Status::InvalidArgument<false>("Object storage bucket is not specified"));
    }
    apply_be_obj_storage_defaults(parsed.client);
    return parsed;
}
} // namespace
void apply_be_obj_storage_defaults(ObjStorageClientConfig& client) {
    normalize_obj_storage_endpoint(client, config::s3_client_http_scheme);
    std::visit(
            []<typename C>(C& provider) {
                auto& connection = provider.connection;
                if (!connection.ca_cert_file) {
                    if (auto ca = be_ca_bundle_path(); !ca.empty()) {
                        connection.ca_cert_file = std::move(ca);
                    }
                }
                if (!connection.max_retries) {
                    connection.max_retries = config::max_s3_client_retry;
                }
                if constexpr (std::is_same_v<C, AzureObjStorageConfig>) {
                    provider.retry_too_many_requests = true;
                } else {
                    if (!connection.max_connections) {
                        connection.max_connections = 102400;
                    }
                    if (!connection.request_timeout) {
                        connection.request_timeout = std::chrono::seconds(30);
                    }
                    provider.retry_slow_down = true;
                    if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
                        if (provider.auth.assume_role &&
                            !provider.auth.assume_role->request_timeout) {
                            provider.auth.assume_role->request_timeout = std::chrono::milliseconds(
                                    config::aws_client_request_timeout_ms);
                        }
                    }
                }
            },
            client);
}

Result<ObjStorageResourceConfig> ObjStorageConfigParser::from_properties(
        const Properties& properties, const S3URI& uri) {
    Properties p;
    for (const auto& [key, value] : properties) {
        auto name = to_upper(key);
        if (!name.starts_with("AWS_")) {
            name = to_lower(key);
        }
        p.try_emplace(std::move(name), value);
    }
    auto provider = Vault::S3;
    const auto name = p.find("provider");
    if (name != p.end() && !trim(name->second).empty()) {
        const auto mapping = std::ranges::find(PROVIDERS, to_upper(std::string(trim(name->second))),
                                               &ProviderMapping::name);
        // Old generic S3 resources sometimes carry an unrelated provider alias.
        if (mapping != std::end(PROVIDERS)) {
            provider = mapping->vault;
        }
    } else if (auto it = p.find("AWS_ENDPOINT"); it != p.end()) {
        provider = storage_provider_from_endpoint(it->second);
    }
    return parse_resource(p, provider, uri.get_bucket(), {});
}

Result<ObjStorageResourceConfig> ObjStorageConfigParser::from_vault(const Vault& info) {
    ObjStorageResourceConfig parsed;
    if (auto e = parse_vault_storage_config(
                info, aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS, &parsed)) {
        return ResultError(Status::InvalidArgument<false>(*e));
    }
    parsed.is_internal_bucket = true;
    apply_be_obj_storage_defaults(parsed.client);
    return parsed;
}

Result<ObjStorageResourceConfig> ObjStorageConfigParser::from_storage_param(
        const TS3StorageParam& param) {
    Vault::Provider provider;
    if (param.provider == TObjStorageType::UNKNOWN) {
        provider = storage_provider_from_endpoint(param.endpoint);
    } else {
        const auto mapping =
                std::ranges::find(PROVIDERS, param.provider, &ProviderMapping::resource);
        if (mapping == std::end(PROVIDERS)) {
            return ResultError(
                    Status::InvalidArgument("Unknown object storage provider {}", param.provider));
        }
        provider = mapping->vault;
    }
    Properties p {
            {"AWS_MAX_CONNECTIONS", std::to_string(param.max_conn)},
            {"AWS_REQUEST_TIMEOUT_MS", std::to_string(param.request_timeout_ms)},
            {"AWS_CONNECTION_TIMEOUT_MS", std::to_string(param.conn_timeout_ms)},
            {"use_path_style", param.use_path_style ? "true" : "false"},
    };
    if (param.__isset.region || !param.region.empty()) {
        p["AWS_REGION"] = param.region;
    }
    if (param.__isset.endpoint || !param.endpoint.empty()) {
        p["AWS_ENDPOINT"] = param.endpoint;
    }
    if (param.__isset.ak || !param.ak.empty()) {
        p["AWS_ACCESS_KEY"] = param.ak;
    }
    if (param.__isset.sk || !param.sk.empty()) {
        p["AWS_SECRET_KEY"] = param.sk;
    }
    if (param.__isset.token || !param.token.empty()) {
        p["AWS_TOKEN"] = param.token;
    }
    if (param.__isset.role_arn || !param.role_arn.empty()) {
        p["AWS_ROLE_ARN"] = param.role_arn;
    }
    if (param.__isset.external_id || !param.external_id.empty()) {
        p["AWS_EXTERNAL_ID"] = param.external_id;
    }
    if (param.__isset.cred_provider_type) {
        auto name = _TCredProviderType_VALUES_TO_NAMES.find(param.cred_provider_type);
        if (name == _TCredProviderType_VALUES_TO_NAMES.end()) {
            return ResultError(Status::InvalidArgument(
                    "Unknown storage credential provider type {}", param.cred_provider_type));
        }
        p["AWS_CREDENTIALS_PROVIDER_TYPE"] = name->second;
    }
    if (param.__isset.credential && param.credential.__isset.gcp_credential) {
        const auto& g = param.credential.gcp_credential;
        auto type = g.__isset.credential_provider_type ? g.credential_provider_type
                                                       : TGcpCredentialProviderType::DEFAULT;
        auto name = _TGcpCredentialProviderType_VALUES_TO_NAMES.find(type);
        if (name == _TGcpCredentialProviderType_VALUES_TO_NAMES.end()) {
            return ResultError(
                    Status::InvalidArgument("Unknown GCP credential provider type {}", type));
        }
        p[std::string(GCP_CREDENTIAL_PROVIDER_TYPE)] = name->second;
        if (g.__isset.impersonation_service_account) {
            p[std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT)] = g.impersonation_service_account;
        }
    }
    return parse_resource(p, provider, param.bucket, param.root_path);
}
} // namespace doris
