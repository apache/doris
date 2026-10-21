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

#include "io/obj_storage/obj_storage_config_resolver.h"

#include <absl/strings/match.h>
#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/cloud.pb.h>
#include <strings.h>

#include <charconv>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>

#include "common/logging.h"
#include "cpp/aws_common.h"
#include "io/obj_storage/azure_config_codec.h"
#include "io/obj_storage/azure_config_validator.h"
#include "io/obj_storage/obj_storage_uri.h"
#include "util/s3_util.h"
#include "util/string_util.h"

namespace doris::io {

namespace {

// The parameters every flat source carries, in that shape (formerly S3ClientConf). It only
// lives here, between reading a source and mapping it into the typed backend.
struct FlatClientFields {
    std::string endpoint;
    std::string region;
    std::string ak;
    std::string sk;
    std::string token;
    // The Azure container; unused by S3, whose clients are not bound to a bucket.
    std::string bucket;
    ObjStorageProvider provider = ObjStorageProvider::AWS;
    int max_connections = -1;
    int request_timeout_ms = -1;
    int connect_timeout_ms = -1;
    bool use_virtual_addressing = true;
    // For aws s3, no need to override endpoint
    bool need_override_endpoint = true;
    CredProviderType cred_provider_type = CredProviderType::Default;
    std::string role_arn;
    std::string external_id;
    bool is_internal_bucket = false;
};

constexpr char FLAT_USE_PATH_STYLE[] = "use_path_style";
constexpr char FLAT_AZURE_PROVIDER_STRING[] = "AZURE";
constexpr char FLAT_PROVIDER[] = "provider";
constexpr char FLAT_AK[] = "AWS_ACCESS_KEY";
constexpr char FLAT_SK[] = "AWS_SECRET_KEY";
constexpr char FLAT_ENDPOINT[] = "AWS_ENDPOINT";
constexpr char FLAT_REGION[] = "AWS_REGION";
constexpr char FLAT_TOKEN[] = "AWS_TOKEN";
constexpr char FLAT_MAX_CONN_SIZE[] = "AWS_MAX_CONNECTIONS";
constexpr char FLAT_REQUEST_TIMEOUT_MS[] = "AWS_REQUEST_TIMEOUT_MS";
constexpr char FLAT_CONN_TIMEOUT_MS[] = "AWS_CONNECTION_TIMEOUT_MS";
constexpr char FLAT_NEED_OVERRIDE_ENDPOINT[] = "AWS_NEED_OVERRIDE_ENDPOINT";
constexpr char FLAT_ROLE_ARN[] = "AWS_ROLE_ARN";
constexpr char FLAT_EXTERNAL_ID[] = "AWS_EXTERNAL_ID";
constexpr char FLAT_CREDENTIALS_PROVIDER_TYPE[] = "AWS_CREDENTIALS_PROVIDER_TYPE";

// Return true is convert `str` to int successfully
bool flat_to_int(std::string_view str, int& res) {
    auto [_, ec] = std::from_chars(str.data(), str.data() + str.size(), res);
    return ec == std::errc {};
}

std::string flat_fields_to_string(const FlatClientFields& fields) {
    return fmt::format(
            "(ak={}, token={}, endpoint={}, region={}, bucket={}, max_connections={}, "
            "request_timeout_ms={}, connect_timeout_ms={}, use_virtual_addressing={}, "
            "cred_provider_type={},role_arn={}, external_id={}, is_internal_bucket={}",
            hide_access_key(fields.ak), fields.token.empty() ? "" : "******", fields.endpoint,
            fields.region, fields.bucket, fields.max_connections, fields.request_timeout_ms,
            fields.connect_timeout_ms, fields.use_virtual_addressing,
            static_cast<int>(fields.cred_provider_type), fields.role_arn, fields.external_id,
            fields.is_internal_bucket);
}

// The rules FE properties have always been checked against when they are converted.
Status validate_flat_fields(const FlatClientFields& fields) {
    if (fields.endpoint.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty endpoint");
    }
    if (fields.region.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty region");
    }
    if (fields.role_arn.empty()) {
        // Either both credentials are provided or both are empty (anonymous access)
        if (!fields.ak.empty() && fields.sk.empty()) {
            return Status::InvalidArgument<false>("Invalid s3 conf, empty sk");
        }
        if (!fields.sk.empty() && fields.ak.empty()) {
            return Status::InvalidArgument<false>("Invalid s3 conf, empty ak");
        }
    }
    return Status::OK();
}

std::variant<S3ClientConfig, AzureClientConfig> flat_backend(const FlatClientFields& fields) {
    if (fields.provider == ObjStorageProvider::AZURE) {
        return AzureClientConfig {
                .endpoint = fields.endpoint,
                .account = fields.ak,
                .container = fields.bucket,
                .auth = AzureSharedKeyConfig {.account_key = fields.sk},
        };
    }
    return S3ClientConfig {
            .vendor = fields.provider,
            .endpoint = fields.endpoint,
            .region = fields.region,
            .need_override_endpoint = fields.need_override_endpoint,
            .use_virtual_addressing = fields.use_virtual_addressing,
            .auth =
                    {
                            .access_key = fields.ak,
                            .secret_key = fields.sk,
                            .session_token = fields.token,
                            .provider_type = fields.cred_provider_type,
                            .role_arn = fields.role_arn,
                            .external_id = fields.external_id,
                    },
    };
}

ObjClientConfig flat_client_config(const FlatClientFields& fields) {
    return ObjClientConfig {
            .connection =
                    {
                            .max_connections = fields.max_connections,
                            .request_timeout_ms = fields.request_timeout_ms,
                            .connect_timeout_ms = fields.connect_timeout_ms,
                    },
            .usage = fields.is_internal_bucket ? ClientUsage::INTERNAL_STORAGE
                                               : ClientUsage::EXTERNAL_ACCESS,
            .backend = flat_backend(fields),
    };
}

CredProviderType flat_cred_provider_type_from_thrift(TCredProviderType::type cred_provider_type) {
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
        __builtin_unreachable();
        LOG(WARNING) << "Invalid TCredProviderType value: " << cred_provider_type
                     << ", use default instead.";
        return CredProviderType::Default;
    }
}

} // namespace

Result<ObjStorageConfig> ObjStorageConfigResolver::from_properties(
        const std::map<std::string, std::string>& prop, const ObjStorageUri& location) {
    StringCaseMap<std::string> properties(prop.begin(), prop.end());
    FlatClientFields fields;
    if (auto it = properties.find(FLAT_AK); it != properties.end()) {
        fields.ak = it->second;
    }
    if (auto it = properties.find(FLAT_SK); it != properties.end()) {
        fields.sk = it->second;
    }
    if (auto it = properties.find(FLAT_TOKEN); it != properties.end()) {
        fields.token = it->second;
    }
    if (auto it = properties.find(FLAT_ENDPOINT); it != properties.end()) {
        fields.endpoint = it->second;
    }
    if (auto it = properties.find(FLAT_NEED_OVERRIDE_ENDPOINT); it != properties.end()) {
        fields.need_override_endpoint = (it->second == "true");
    }
    if (auto it = properties.find(FLAT_REGION); it != properties.end()) {
        fields.region = it->second;
    }
    if (auto it = properties.find(FLAT_MAX_CONN_SIZE); it != properties.end()) {
        if (!flat_to_int(it->second, fields.max_connections)) {
            return ResultError(Status::InvalidArgument("invalid {} value \"{}\"",
                                                       FLAT_MAX_CONN_SIZE, it->second));
        }
    }
    if (auto it = properties.find(FLAT_REQUEST_TIMEOUT_MS); it != properties.end()) {
        if (!flat_to_int(it->second, fields.request_timeout_ms)) {
            return ResultError(Status::InvalidArgument("invalid {} value \"{}\"",
                                                       FLAT_REQUEST_TIMEOUT_MS, it->second));
        }
    }
    if (auto it = properties.find(FLAT_CONN_TIMEOUT_MS); it != properties.end()) {
        if (!flat_to_int(it->second, fields.connect_timeout_ms)) {
            return ResultError(Status::InvalidArgument("invalid {} value \"{}\"",
                                                       FLAT_CONN_TIMEOUT_MS, it->second));
        }
    }
    if (auto it = properties.find(FLAT_PROVIDER); it != properties.end()) {
        // S3 Provider properties should be case insensitive.
        if (0 == strcasecmp(it->second.c_str(), FLAT_AZURE_PROVIDER_STRING)) {
            fields.provider = ObjStorageProvider::AZURE;
        }
    }

    if (location.bucket.empty()) {
        return ResultError(Status::InvalidArgument("Invalid S3 URI {}, bucket is not specified",
                                                   location.original));
    }
    // For azure's compatibility
    fields.bucket = location.bucket;

    // See https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_s3_1_1_s3_client.html
    fields.use_virtual_addressing = true;
    if (auto it = properties.find(FLAT_USE_PATH_STYLE); it != properties.end()) {
        fields.use_virtual_addressing = it->second != "true";
    }
    if (auto it = properties.find(FLAT_ROLE_ARN); it != properties.end()) {
        // Keep provider type as Default unless explicitly configured by
        // AWS_CREDENTIALS_PROVIDER_TYPE, consistent with FE behavior.
        fields.role_arn = it->second;
    }
    if (auto it = properties.find(FLAT_EXTERNAL_ID); it != properties.end()) {
        fields.external_id = it->second;
    }
    if (auto it = properties.find(FLAT_CREDENTIALS_PROVIDER_TYPE); it != properties.end()) {
        fields.cred_provider_type = cred_provider_type_from_string(it->second);
    }

    RETURN_IF_ERROR_RESULT(validate_flat_fields(fields));
    return ObjStorageConfig {
            .bucket = location.bucket,
            .prefix = "",
            .client = flat_client_config(fields),
    };
}

ObjStorageConfig ObjStorageConfigResolver::from_object_store_info(
        const cloud::ObjectStoreInfoPB& info) {
    FlatClientFields fields {
            .endpoint = info.endpoint(),
            .region = info.region(),
            .ak = info.ak(),
            .sk = info.sk(),
            .token = {},
            .bucket = info.bucket(),
            .provider = ObjStorageProvider::AWS,
            .use_virtual_addressing = info.has_use_path_style() ? !info.use_path_style() : true,
            .role_arn = info.role_arn(),
            .external_id = info.external_id(),
            // ObjectStoreInfoPB always describes a storage vault, i.e. a Doris
            // internal bucket in cloud mode.
            .is_internal_bucket = true,
    };
    if (info.has_cred_provider_type()) {
        fields.cred_provider_type = cred_provider_type_from_pb(info.cred_provider_type());
    }

    switch (info.provider()) {
    case cloud::ObjectStoreInfoPB_Provider_OSS:
        fields.provider = ObjStorageProvider::OSS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_S3:
        fields.provider = ObjStorageProvider::AWS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_COS:
        fields.provider = ObjStorageProvider::COS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_OBS:
        fields.provider = ObjStorageProvider::OBS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_BOS:
        fields.provider = ObjStorageProvider::BOS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_GCP:
        fields.provider = ObjStorageProvider::GCP;
        break;
    case cloud::ObjectStoreInfoPB_Provider_AZURE:
        fields.provider = ObjStorageProvider::AZURE;
        break;
    case cloud::ObjectStoreInfoPB_Provider_TOS:
        fields.provider = ObjStorageProvider::TOS;
        break;
    default:
        __builtin_unreachable();
        LOG_FATAL("unknown provider type {}, info {}", info.provider(),
                  flat_fields_to_string(fields));
    }
    return ObjStorageConfig {
            .bucket = info.bucket(),
            .prefix = info.prefix(),
            .client = flat_client_config(fields),
    };
}

ObjStorageConfig ObjStorageConfigResolver::from_storage_param(const TS3StorageParam& param) {
    FlatClientFields fields {
            .endpoint = param.endpoint,
            .region = param.region,
            .ak = param.ak,
            .sk = param.sk,
            .token = param.token,
            .bucket = param.bucket,
            .provider = ObjStorageProvider::AWS,
            .max_connections = param.max_conn,
            .request_timeout_ms = param.request_timeout_ms,
            .connect_timeout_ms = param.conn_timeout_ms,
            // When using cold heat separation in minio, user might use ip address directly,
            // which needs enable use_virtual_addressing to true
            .use_virtual_addressing = !param.use_path_style,
            .role_arn = param.role_arn,
            .external_id = param.external_id,
    };
    if (param.__isset.cred_provider_type) {
        fields.cred_provider_type = flat_cred_provider_type_from_thrift(param.cred_provider_type);
    }

    switch (param.provider) {
    case TObjStorageType::UNKNOWN:
        LOG_INFO("Receive one legal storage resource, set provider type to aws, param detail {}",
                 flat_fields_to_string(fields));
        fields.provider = ObjStorageProvider::AWS;
        break;
    case TObjStorageType::AWS:
        fields.provider = ObjStorageProvider::AWS;
        break;
    case TObjStorageType::AZURE:
        fields.provider = ObjStorageProvider::AZURE;
        break;
    case TObjStorageType::BOS:
        fields.provider = ObjStorageProvider::BOS;
        break;
    case TObjStorageType::COS:
        fields.provider = ObjStorageProvider::COS;
        break;
    case TObjStorageType::OBS:
        fields.provider = ObjStorageProvider::OBS;
        break;
    case TObjStorageType::OSS:
        fields.provider = ObjStorageProvider::OSS;
        break;
    case TObjStorageType::GCP:
        fields.provider = ObjStorageProvider::GCP;
        break;
    case TObjStorageType::TOS:
        fields.provider = ObjStorageProvider::TOS;
        break;
    default:
        LOG_FATAL("unknown provider type {}, info {}", param.provider,
                  flat_fields_to_string(fields));
        __builtin_unreachable();
    }
    return ObjStorageConfig {
            .bucket = param.bucket,
            .prefix = param.root_path,
            .client = flat_client_config(fields),
    };
}

Result<ObjClientConfig> ObjStorageConfigResolver::from_azure_client_config(
        const TAzureClientConfig& config, const ObjStorageUri& location) {
    auto azure = AzureConfigCodec::decode(config, location.bucket);
    if (!azure.has_value()) {
        return ResultError(std::move(azure).error());
    }
    // The account requests go to: the configured one, or for SAS/client secret without
    // account_name the one the endpoint names. Either side may name none (an s3-style location
    // has no host, a custom endpoint names no account), and then there is nothing to compare.
    std::string_view target_account =
            azure->account.empty() ? ObjStorageUri::azure_account_of_endpoint(azure->endpoint)
                                   : std::string_view(azure->account);
    if (!location.account.empty() && !target_account.empty() &&
        !absl::EqualsIgnoreCase(location.account, target_account)) {
        return ResultError(Status::InvalidArgument<false>(
                "Azure account '{}' of location {} does not match the account '{}' of the "
                "client config",
                location.account, location.original, target_account));
    }
    RETURN_IF_ERROR_RESULT(AzureConfigValidator::validate(azure.value()));
    return ObjClientConfig {
            .connection = {},
            .usage = ClientUsage::EXTERNAL_ACCESS,
            .backend = std::move(azure).value(),
    };
}

Result<ObjClientConfig> ObjStorageConfigResolver::merge_update(const ObjClientConfig& current,
                                                               const ObjClientConfig& update) {
    if (current.backend.index() != update.backend.index()) {
        return ResultError(Status::InvalidArgument<false>(
                "storage backend changed from {} to {}; the storage target cannot be reset in "
                "place",
                current.to_string(), update.to_string()));
    }
    ObjClientConfig merged = current;
    merged.connection = update.connection;
    merged.usage = update.usage;
    if (auto* s3 = std::get_if<S3ClientConfig>(&merged.backend)) {
        const auto& update_s3 = std::get<S3ClientConfig>(update.backend);
        s3->use_virtual_addressing = update_s3.use_virtual_addressing;
        s3->auth = update_s3.auth;
    } else {
        auto& azure = std::get<AzureClientConfig>(merged.backend);
        const auto& update_azure = std::get<AzureClientConfig>(update.backend);
        azure.account = update_azure.account;
        azure.container = update_azure.container;
        azure.auth = update_azure.auth;
    }
    return merged;
}

} // namespace doris::io
