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
#include <chrono>
#include <type_traits>

#include "common/config.h"
#include "cpp/obj-storage/auth/gcp/gcp_auth.h"
#include "cpp/obj-storage/obj_storage_properties.h"
#include "io/object_storage/aws_client_settings.h"
#include "util/s3_uri.h"
#include "util/string_util.h"

namespace doris {
namespace {
using Vault = cloud::ObjectStoreInfoPB;

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

void set_sts_timeout(std::optional<AwsAssumeRole>& role) {
    if (role && !role->request_timeout) {
        role->request_timeout = std::chrono::milliseconds(config::aws_client_request_timeout_ms);
    }
}

void apply_be_defaults(ObjStorageClientConfig& client) {
    normalize_obj_storage_endpoint(client, config::s3_client_http_scheme);
    auto& conn = obj_storage_connection(client);
    if (auto ca = be_ca_bundle_path(); !ca.empty()) {
        conn.ca_cert_file = std::move(ca);
    }
    conn.max_retries = config::max_s3_client_retry;
    std::visit(
            []<class C>(C& settings) {
                if constexpr (std::is_same_v<C, AzureObjStorageConfig>) {
                    settings.retry_too_many_requests = true;
                } else {
                    if (!settings.connection.max_connections) {
                        settings.connection.max_connections = 102400;
                    }
                    if (!settings.connection.request_timeout) {
                        settings.connection.request_timeout = std::chrono::seconds(30);
                    }
                    settings.retry_slow_down = true;
                    if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
                        set_sts_timeout(settings.auth.assume_role);
                    } else if (auto* chain = std::get_if<S3CredentialChain>(&settings.auth)) {
                        set_sts_timeout(chain->assume_role);
                    }
                }
            },
            client);
}

Result<ObjStorageResourceConfig> parse_be_resource(const ObjStorageProperties& properties,
                                                   Vault::Provider provider, std::string bucket,
                                                   std::string prefix) {
    ObjStorageResourceConfig resource {.bucket = std::move(bucket), .prefix = std::move(prefix)};
    if (auto error = parse_obj_storage_client_config(
                provider, properties,
                aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS, &resource.client)) {
        return ResultError(Status::InvalidArgument<false>(*error));
    }
    if (resource.bucket.empty()) {
        return ResultError(
                Status::InvalidArgument<false>("object storage bucket is not specified"));
    }
    apply_be_defaults(resource.client);
    return resource;
}
} // namespace

Result<ObjStorageResourceConfig> ObjStorageConfigParser::from_properties(
        const std::map<std::string, std::string>& input, const S3URI& uri) {
    ObjStorageProperties properties;
    for (const auto& [key, v] : input) {
        auto normalized = to_upper(key);
        if (!normalized.starts_with("AWS_")) {
            normalized = to_lower(key);
        }
        properties.try_emplace(std::move(normalized), v);
    }
    auto provider = Vault::S3;
    const auto name = properties.find("provider");
    if (name != properties.end() && !trim(name->second).empty()) {
        const auto entry = std::ranges::find(PROVIDERS, to_upper(std::string(trim(name->second))),
                                             &ProviderMapping::name);
        if (entry != std::end(PROVIDERS)) {
            provider = entry->vault;
        }
    } else if (auto endpoint = properties.find("AWS_ENDPOINT"); endpoint != properties.end()) {
        provider = storage_provider_from_endpoint(endpoint->second);
    }
    return parse_be_resource(properties, provider, uri.get_bucket(), {});
}

Result<ObjStorageResourceConfig> ObjStorageConfigParser::from_vault(const Vault& info) {
    ObjStorageResourceConfig resource;
    if (auto error = parse_vault_storage_config(
                info, aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::ANONYMOUS, &resource)) {
        return ResultError(Status::InvalidArgument<false>(*error));
    }
    resource.is_internal_bucket = true;
    apply_be_defaults(resource.client);
    return resource;
}

Result<ObjStorageResourceConfig> ObjStorageConfigParser::from_storage_param(
        const TS3StorageParam& param) {
    Vault::Provider provider;
    if (param.provider == TObjStorageType::UNKNOWN) {
        provider = storage_provider_from_endpoint(param.endpoint);
    } else {
        auto entry = std::ranges::find(PROVIDERS, param.provider, &ProviderMapping::resource);
        if (entry == std::end(PROVIDERS)) {
            return ResultError(Status::InvalidArgument("unknown storage resource provider {}",
                                                       param.provider));
        }
        provider = entry->vault;
    }

    // The generated Thrift fields carry their protocol defaults even when omitted.
    ObjStorageProperties properties {
            {"AWS_REQUEST_TIMEOUT_MS", std::to_string(param.request_timeout_ms)},
            {"AWS_CONNECTION_TIMEOUT_MS", std::to_string(param.conn_timeout_ms)},
            {"use_path_style", param.use_path_style ? "true" : "false"},
    };
    // S3ThriftAdapter always writes the Thrift default pool size, including for Azure.
    // Only a non-default value on that wire represents an Azure pool-size request.
    if (provider != Vault::AZURE || param.max_conn != TS3StorageParam {}.max_conn) {
        properties["AWS_MAX_CONNECTIONS"] = std::to_string(param.max_conn);
    }
    if (param.__isset.endpoint || !param.endpoint.empty()) {
        properties["AWS_ENDPOINT"] = param.endpoint;
    }
    if (param.__isset.region || !param.region.empty()) {
        properties["AWS_REGION"] = param.region;
    }
    if (param.__isset.ak || !param.ak.empty()) {
        properties["AWS_ACCESS_KEY"] = param.ak;
    }
    if (param.__isset.sk || !param.sk.empty()) {
        properties["AWS_SECRET_KEY"] = param.sk;
    }
    if (param.__isset.role_arn || !param.role_arn.empty()) {
        properties["AWS_ROLE_ARN"] = param.role_arn;
    }
    if (param.__isset.external_id || !param.external_id.empty()) {
        properties["AWS_EXTERNAL_ID"] = param.external_id;
    }
    if (param.__isset.token || !param.token.empty()) {
        properties["AWS_TOKEN"] = param.token;
    }

    if (param.__isset.cred_provider_type) {
        auto name = _TCredProviderType_VALUES_TO_NAMES.find(param.cred_provider_type);
        if (name == _TCredProviderType_VALUES_TO_NAMES.end()) {
            return ResultError(Status::InvalidArgument("unknown credential provider {}",
                                                       param.cred_provider_type));
        }
        properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = name->second;
    }
    if (param.__isset.credential && param.credential.__isset.gcp_credential) {
        const auto& gcp = param.credential.gcp_credential;
        auto type = gcp.__isset.credential_provider_type ? gcp.credential_provider_type
                                                         : TGcpCredentialProviderType::DEFAULT;
        auto name = _TGcpCredentialProviderType_VALUES_TO_NAMES.find(type);
        if (name == _TGcpCredentialProviderType_VALUES_TO_NAMES.end()) {
            return ResultError(Status::InvalidArgument("unknown GCP credential provider {}", type));
        }
        properties[std::string(GCP_CREDENTIAL_PROVIDER_TYPE)] = name->second;
        if (gcp.__isset.impersonation_service_account) {
            properties[std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT)] =
                    gcp.impersonation_service_account;
        }
    }
    return parse_be_resource(properties, provider, param.bucket, param.root_path);
}
} // namespace doris
