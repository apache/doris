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
#include <charconv>
#include <iterator>
#include <string_view>
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

ObjStorageConfig make_config(cloud::ObjectStoreInfoPB::Provider provider) {
    auto config = make_vault_storage_config(provider);
    if (!config) {
        LOG_FATAL("unknown object storage provider {}", provider);
        __builtin_unreachable();
    }
    return std::move(*config);
}

ObjStorageConfig select_provider_config(const StringCaseMap<std::string>& properties,
                                        const ObjStorageConfig& defaults) {
    cloud::ObjectStoreInfoPB::Provider provider;
    const auto name = properties.find(S3_PROVIDER);
    if (name != properties.end() && !trim(name->second).empty()) {
        const auto entry = std::ranges::find(PROVIDERS, to_upper(std::string(trim(name->second))),
                                             &ProviderMapping::name);
        if (entry == std::end(PROVIDERS)) {
            return defaults;
        }
        provider = entry->vault;
    } else {
        const auto endpoint = properties.find(S3_ENDPOINT);
        if (endpoint == properties.end()) {
            return defaults;
        }
        provider = storage_provider_from_endpoint(endpoint->second);
        if (provider == cloud::ObjectStoreInfoPB::S3) {
            // A generic S3 endpoint carries no information about the cloud.
            return defaults;
        }
    }

    auto selected = make_config(provider);
    if (selected.index() == defaults.index()) {
        return defaults;
    }
    storage_config_base(selected) = storage_config_base(defaults);
    return selected;
}

} // namespace

Status ObjStorageConfigParser::from_properties(const std::map<std::string, std::string>& prop,
                                               const S3URI& s3_uri,
                                               ObjStorageConfig* storage_config) {
    StringCaseMap<std::string> properties(prop.begin(), prop.end());
    auto parsed = select_provider_config(properties, *storage_config);
    auto& config = storage_config_base(parsed);
    if (auto endpoint = properties.find(S3_ENDPOINT); endpoint != properties.end()) {
        config.endpoint = endpoint->second;
    }
    if (auto it = properties.find(S3_AK); it != properties.end()) {
        config.ak = it->second;
    }
    if (auto it = properties.find(S3_SK); it != properties.end()) {
        config.sk = it->second;
    }
    if (auto it = properties.find(S3_TOKEN); it != properties.end()) {
        config.token = it->second;
    }
    if (auto it = properties.find(S3_NEED_OVERRIDE_ENDPOINT); it != properties.end()) {
        config.need_override_endpoint = (it->second == "true");
    }
    if (auto it = properties.find(S3_REGION); it != properties.end()) {
        config.region = it->second;
    }
    if (auto it = properties.find(S3_MAX_CONN_SIZE); it != properties.end()) {
        if (!to_int(it->second, config.max_connections)) {
            return Status::InvalidArgument("invalid {} value \"{}\"", S3_MAX_CONN_SIZE, it->second);
        }
    }
    if (auto it = properties.find(S3_REQUEST_TIMEOUT_MS); it != properties.end()) {
        if (!to_int(it->second, config.request_timeout_ms)) {
            return Status::InvalidArgument("invalid {} value \"{}\"", S3_REQUEST_TIMEOUT_MS,
                                           it->second);
        }
    }
    if (auto it = properties.find(S3_CONN_TIMEOUT_MS); it != properties.end()) {
        if (!to_int(it->second, config.connect_timeout_ms)) {
            return Status::InvalidArgument("invalid {} value \"{}\"", S3_CONN_TIMEOUT_MS,
                                           it->second);
        }
    }
    if (s3_uri.get_bucket().empty()) {
        return Status::InvalidArgument("Invalid S3 URI {}, bucket is not specified",
                                       s3_uri.to_string());
    }
    config.bucket = s3_uri.get_bucket();
    config.prefix.clear();
    config.use_virtual_addressing = true;
    if (auto it = properties.find(USE_PATH_STYLE); it != properties.end()) {
        config.use_virtual_addressing = it->second != "true";
    }

    if (auto it = properties.find(S3_ROLE_ARN); it != properties.end()) {
        // Keep provider type as Default unless explicitly configured by
        // AWS_CREDENTIALS_PROVIDER_TYPE, consistent with FE behavior.
        config.role_arn = it->second;
    }

    if (auto it = properties.find(S3_EXTERNAL_ID); it != properties.end()) {
        config.external_id = it->second;
    }

    if (auto it = properties.find(S3_CREDENTIALS_PROVIDER_TYPE); it != properties.end()) {
        config.cred_provider_type = cred_provider_type_from_string(it->second);
    }

    if (auto error = parse_obj_credential_properties(properties, &config.credential);
        error.has_value()) {
        return Status::InvalidArgument(*error);
    }

    if (auto error = validate_obj_storage_config(parsed)) {
        return Status::InvalidArgument<false>(*error);
    }
    *storage_config = std::move(parsed);
    return Status::OK();
}

static CredProviderType cred_provider_type_from_thrift(TCredProviderType::type cred_provider_type) {
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

ObjStorageConfig ObjStorageConfigParser::from_vault(const cloud::ObjectStoreInfoPB& info) {
    auto parsed = make_config(vault_storage_provider(info));
    auto& config = storage_config_base(parsed);
    config.bucket = info.bucket();
    config.prefix = info.prefix();
    config.sse_enabled = info.sse_enabled();
    config.is_internal_bucket = true;
    config.endpoint = info.endpoint();
    config.region = info.region();
    config.ak = info.ak();
    config.sk = info.sk();
    config.use_virtual_addressing = !info.use_path_style();
    config.role_arn = info.role_arn();
    config.external_id = info.external_id();
    if (info.has_cred_provider_type()) {
        config.cred_provider_type = cred_provider_type_from_pb(info.cred_provider_type());
    }
    convert_obj_credential(info, &config.credential);
    return parsed;
}

ObjStorageConfig ObjStorageConfigParser::from_storage_param(const TS3StorageParam& param) {
    cloud::ObjectStoreInfoPB::Provider vault_provider;
    if (param.provider == TObjStorageType::UNKNOWN) {
        vault_provider = storage_provider_from_endpoint(param.endpoint);
    } else {
        const auto provider =
                std::ranges::find(PROVIDERS, param.provider, &ProviderMapping::resource);
        if (provider == std::end(PROVIDERS)) {
            LOG_FATAL("unknown storage resource provider {}", param.provider);
            __builtin_unreachable();
        }
        vault_provider = provider->vault;
    }
    auto parsed = make_config(vault_provider);
    auto& config = storage_config_base(parsed);
    config.bucket = param.bucket;
    config.prefix = param.root_path;
    config.endpoint = param.endpoint;
    config.region = param.region;
    config.ak = param.ak;
    config.sk = param.sk;
    config.token = param.token;
    config.max_connections = param.max_conn;
    config.request_timeout_ms = param.request_timeout_ms;
    config.connect_timeout_ms = param.conn_timeout_ms;
    config.use_virtual_addressing = !param.use_path_style;
    config.role_arn = param.role_arn;
    config.external_id = param.external_id;
    if (param.__isset.cred_provider_type) {
        config.cred_provider_type = cred_provider_type_from_thrift(param.cred_provider_type);
    }
    if (param.__isset.credential) {
        convert_obj_credential(param.credential, &config.credential);
    }
    return parsed;
}

} // namespace doris
