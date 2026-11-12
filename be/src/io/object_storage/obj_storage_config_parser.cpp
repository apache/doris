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

#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/cloud.pb.h>

#include <algorithm>
#include <charconv>
#include <iterator>
#include <string_view>
#include <type_traits>
#include <utility>

#include "util/s3_uri.h"
#include "util/string_util.h"

namespace doris {
namespace {

// Return true is convert `str` to int successfully
bool to_int(std::string_view str, int& res) {
    auto [_, ec] = std::from_chars(str.data(), str.data() + str.size(), res);
    return ec == std::errc {};
}

constexpr char USE_PATH_STYLE[] = "use_path_style";

constexpr char S3_PROVIDER[] = "provider";
constexpr char S3_AK[] = "AWS_ACCESS_KEY";
constexpr char S3_SK[] = "AWS_SECRET_KEY";
constexpr char S3_ENDPOINT[] = "AWS_ENDPOINT";
constexpr char S3_REGION[] = "AWS_REGION";
constexpr char S3_TOKEN[] = "AWS_TOKEN";
constexpr char S3_MAX_CONN_SIZE[] = "AWS_MAX_CONNECTIONS";
constexpr char S3_REQUEST_TIMEOUT_MS[] = "AWS_REQUEST_TIMEOUT_MS";
constexpr char S3_CONN_TIMEOUT_MS[] = "AWS_CONNECTION_TIMEOUT_MS";
constexpr char S3_NEED_OVERRIDE_ENDPOINT[] = "AWS_NEED_OVERRIDE_ENDPOINT";

constexpr char S3_ROLE_ARN[] = "AWS_ROLE_ARN";
constexpr char S3_EXTERNAL_ID[] = "AWS_EXTERNAL_ID";
constexpr char S3_CREDENTIALS_PROVIDER_TYPE[] = "AWS_CREDENTIALS_PROVIDER_TYPE";

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

CredProviderType cred_provider_type_from_thrift(TCredProviderType::type cred_provider_type) {
    switch (cred_provider_type) {
    case TCredProviderType::DEFAULT:
        return CredProviderType::Default;
    case TCredProviderType::SIMPLE:
        return CredProviderType::Simple;
    case TCredProviderType::INSTANCE_PROFILE:
        return CredProviderType::InstanceProfile;
    case TCredProviderType::ENV:
        return CredProviderType::Env;
    case TCredProviderType::SYSTEM_PROPERTIES:
        return CredProviderType::SystemProperties;
    case TCredProviderType::WEB_IDENTITY:
        return CredProviderType::WebIdentity;
    case TCredProviderType::CONTAINER:
        return CredProviderType::Container;
    case TCredProviderType::ANONYMOUS:
        return CredProviderType::Anonymous;
    default:
        LOG(WARNING) << "Invalid TCredProviderType value: " << cred_provider_type
                     << ", use default instead.";
        return CredProviderType::Default;
    }
}

using Properties = StringCaseMap<std::string>;
using Vault = cloud::ObjectStoreInfoPB;

std::optional<Vault::Provider> property_provider(const Properties& properties) {
    const auto name = properties.find(S3_PROVIDER);
    if (name != properties.end() && !trim(name->second).empty()) {
        const auto entry = std::ranges::find(PROVIDERS, to_upper(std::string(trim(name->second))),
                                             &ProviderMapping::name);
        if (entry != std::end(PROVIDERS)) {
            return entry->vault;
        }
    } else if (const auto endpoint = properties.find(S3_ENDPOINT); endpoint != properties.end()) {
        auto provider = storage_provider_from_endpoint(endpoint->second);
        if (provider != Vault::S3) {
            return provider;
        }
    }
    // An unrecognized provider name or a generic S3 endpoint preserves the caller's default.
    return std::nullopt;
}

void read_property(const Properties& properties, const char* name, std::string* value) {
    if (auto it = properties.find(name); it != properties.end()) {
        *value = it->second;
    }
}

std::optional<std::string> parse_properties(const Properties&, AzureConfig&) {
    return std::nullopt;
}

std::optional<std::string> parse_properties(const Properties& properties,
                                            S3CompatibleConfig& config) {
    read_property(properties, S3_TOKEN, &config.token);
    if (auto it = properties.find(S3_CREDENTIALS_PROVIDER_TYPE); it != properties.end()) {
        config.cred_provider_type = cred_provider_type_from_string(it->second);
    }
    if (auto it = properties.find(S3_NEED_OVERRIDE_ENDPOINT); it != properties.end()) {
        config.need_override_endpoint = (it->second == "true");
    }
    config.use_virtual_addressing = true;
    if (auto it = properties.find(USE_PATH_STYLE); it != properties.end()) {
        config.use_virtual_addressing = it->second != "true";
    }
    for (const auto& [name, value] : {std::pair {S3_MAX_CONN_SIZE, &config.max_connections},
                                      std::pair {S3_REQUEST_TIMEOUT_MS, &config.request_timeout_ms},
                                      std::pair {S3_CONN_TIMEOUT_MS, &config.connect_timeout_ms}}) {
        if (auto it = properties.find(name);
            it != properties.end() && !to_int(it->second, *value)) {
            return fmt::format("invalid {} value \"{}\"", name, it->second);
        }
    }
    return std::nullopt;
}

std::optional<std::string> parse_properties(const Properties& properties, AwsConfig& config) {
    read_property(properties, S3_ROLE_ARN, &config.role_arn);
    read_property(properties, S3_EXTERNAL_ID, &config.external_id);
    return parse_properties(properties, static_cast<S3CompatibleConfig&>(config));
}

std::optional<std::string> parse_properties(const Properties& properties, GcpConfig& config) {
    if (auto error = parse_properties(properties, static_cast<S3CompatibleConfig&>(config))) {
        return error;
    }
    auto credential = parse_gcp_credential_properties(make_property_getter(properties));
    if (credential.error) {
        return credential.error;
    }
    if (credential.credential) {
        config.credential = std::move(*credential.credential);
    }
    auto value = [&](const char* name) -> std::string_view {
        auto it = properties.find(name);
        return it == properties.end() ? std::string_view {} : std::string_view(it->second);
    };
    return config.validate_credential(value(S3_ROLE_ARN), value(S3_EXTERNAL_ID));
}

void parse_storage_param(const TS3StorageParam&, AzureConfig&) {}

void parse_storage_param(const TS3StorageParam& param, S3CompatibleConfig& config) {
    config.token = param.token;
    if (param.__isset.cred_provider_type) {
        config.cred_provider_type = cred_provider_type_from_thrift(param.cred_provider_type);
    }
    config.max_connections = param.max_conn;
    config.request_timeout_ms = param.request_timeout_ms;
    config.connect_timeout_ms = param.conn_timeout_ms;
    config.use_virtual_addressing = !param.use_path_style;
}

void parse_storage_param(const TS3StorageParam& param, AwsConfig& config) {
    parse_storage_param(param, static_cast<S3CompatibleConfig&>(config));
    config.role_arn = param.role_arn;
    config.external_id = param.external_id;
}

void parse_storage_param(const TS3StorageParam& param, GcpConfig& config) {
    parse_storage_param(param, static_cast<S3CompatibleConfig&>(config));
    if (param.__isset.credential && param.credential.__isset.gcp_credential) {
        config.credential = to_gcp_credential_config(param.credential.gcp_credential);
    }
}

void copy_common_options(ObjStorageConfigBase& dst, const ObjStorageConfigBase& src) {
    dst = src;
}

void copy_common_options(S3CompatibleConfig& dst, const S3CompatibleConfig& src) {
    dst = src;
}

template <typename Config>
Config property_defaults(const ObjStorageConfig& defaults) {
    if (const auto* existing = std::get_if<Config>(&defaults)) {
        return *existing;
    }
    Config config;
    std::visit([&](const auto& existing) { copy_common_options(config, existing); }, defaults);
    return config;
}

} // namespace

Status ObjStorageConfigParser::from_properties(const std::map<std::string, std::string>& prop,
                                               const S3URI& uri, ObjStorageConfig* storage_config) {
    Properties properties(prop.begin(), prop.end());
    auto parse = [&]<typename Config>(Config config) -> std::optional<std::string> {
        read_property(properties, S3_ENDPOINT, &config.endpoint);
        read_property(properties, S3_REGION, &config.region);
        read_property(properties, S3_AK, &config.ak);
        read_property(properties, S3_SK, &config.sk);
        if constexpr (!std::is_same_v<Config, GcpConfig>) {
            if (properties.contains(std::string(GCP_CREDENTIAL_PROVIDER_TYPE)) ||
                properties.contains(std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT))) {
                return "GCP credentials require provider=GCP";
            }
        }
        if (auto error = parse_properties(properties, config)) {
            return error;
        }
        if (uri.get_bucket().empty()) {
            return fmt::format("Invalid S3 URI {}, bucket is not specified", uri.to_string());
        }
        config.bucket = uri.get_bucket();
        config.prefix.clear();
        ObjStorageConfig parsed = std::move(config);
        if (auto error = validate_obj_storage_config(parsed)) {
            return error;
        }
        *storage_config = std::move(parsed);
        return std::nullopt;
    };
    auto provider = property_provider(properties);
    auto error = provider ? parse_storage_provider(
                                    *provider,
                                    [&](auto selected) {
                                        return parse(property_defaults<decltype(selected)>(
                                                *storage_config));
                                    })
                          : std::visit(parse, *storage_config);
    if (error) {
        return Status::InvalidArgument<false>(*error);
    }
    return Status::OK();
}

Result<ObjStorageConfig> ObjStorageConfigParser::from_vault(const cloud::ObjectStoreInfoPB& info) {
    ObjStorageConfig result;
    if (auto error = parse_vault_storage_config(info, &result)) {
        return ResultError(Status::InvalidArgument<false>(*error));
    }
    return result;
}

Result<ObjStorageConfig> ObjStorageConfigParser::from_storage_param(const TS3StorageParam& param) {
    Vault::Provider vault_provider;
    if (param.provider == TObjStorageType::UNKNOWN) {
        vault_provider = storage_provider_from_endpoint(param.endpoint);
    } else {
        const auto provider =
                std::ranges::find(PROVIDERS, param.provider, &ProviderMapping::resource);
        if (provider == std::end(PROVIDERS)) {
            return ResultError(Status::InvalidArgument("unknown storage resource provider {}",
                                                       param.provider));
        }
        vault_provider = provider->vault;
    }
    ObjStorageConfig parsed;
    auto error = parse_storage_provider(
            vault_provider, [&]<typename Config>(Config config) -> std::optional<std::string> {
                config.bucket = param.bucket;
                config.prefix = param.root_path;
                config.endpoint = param.endpoint;
                config.region = param.region;
                config.ak = param.ak;
                config.sk = param.sk;
                parse_storage_param(param, config);
                if constexpr (std::is_same_v<Config, GcpConfig>) {
                    if (auto error =
                                config.validate_credential(param.role_arn, param.external_id)) {
                        return error;
                    }
                } else if (param.__isset.credential && param.credential.__isset.gcp_credential) {
                    return "GCP credentials require provider=GCP";
                }
                parsed = std::move(config);
                return std::nullopt;
            });
    if (error) {
        return ResultError(Status::InvalidArgument<false>(*error));
    }
    return parsed;
}

} // namespace doris
