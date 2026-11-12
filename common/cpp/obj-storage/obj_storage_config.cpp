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

#include "cpp/obj-storage/obj_storage_config.h"

#include <brpc/uri.h>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <functional>
#include <tuple>
#include <utility>

namespace doris {
namespace {

// Adapter between the IO storage-domain enum and the credential layer. Keep it
// local: neither layer should own or depend on the other's enum definition.
ObjCredentialProvider to_obj_credential_provider(ObjStorageProvider provider) {
    switch (provider) {
    case ObjStorageProvider::AWS:
        return ObjCredentialProvider::Aws;
    case ObjStorageProvider::AZURE:
        return ObjCredentialProvider::Azure;
    case ObjStorageProvider::BOS:
        return ObjCredentialProvider::Bos;
    case ObjStorageProvider::COS:
        return ObjCredentialProvider::Cos;
    case ObjStorageProvider::GCP:
        return ObjCredentialProvider::Gcp;
    case ObjStorageProvider::OBS:
        return ObjCredentialProvider::Obs;
    case ObjStorageProvider::OSS:
        return ObjCredentialProvider::Oss;
    case ObjStorageProvider::TOS:
        return ObjCredentialProvider::Tos;
    case ObjStorageProvider::UNKNOWN:
        return ObjCredentialProvider::Unknown;
    }
    return ObjCredentialProvider::Unknown;
}

std::string_view pop_dns_label(std::string_view& name) {
    const auto dot = name.rfind('.');
    if (dot == std::string_view::npos) {
        return std::exchange(name, std::string_view {});
    }
    auto label = name.substr(dot + 1);
    name = name.substr(0, dot);
    return label;
}

} // namespace

std::string_view ObjStorageConfigBase::normalized_prefix() const {
    const auto first = prefix.find_first_not_of('/');
    if (first == std::string::npos) {
        return {};
    }
    return std::string_view(prefix).substr(first, prefix.find_last_not_of('/') - first + 1);
}

bool AzureConfig::operator==(const AzureConfig& other) const {
    // The role affects AK/SK validation when refreshing a holder.
    auto fields = [](const AzureConfig& config) {
        return std::tie(config.bucket, config.prefix, config.sse_enabled, config.is_internal_bucket,
                        config.endpoint, config.region, config.ak, config.sk, config.credential,
                        config.role_arn);
    };
    return fields(*this) == fields(other);
}

const ObjStorageConfigBase& storage_config_base(const ObjStorageConfig& config) {
    return std::visit([](const auto& provider) -> const ObjStorageConfigBase& { return provider; },
                      config);
}

ObjStorageConfigBase& storage_config_base(ObjStorageConfig& config) {
    return std::visit([](auto& provider) -> ObjStorageConfigBase& { return provider; }, config);
}

ObjStorageProvider storage_provider(const ObjStorageConfig& config) {
    return std::visit([](const auto& provider) { return provider.provider; }, config);
}

std::optional<std::string> validate_obj_storage_config(const ObjStorageConfig& config) {
    const auto& fields = storage_config_base(config);
    if (fields.endpoint.empty()) {
        return "Invalid s3 conf, empty endpoint";
    }
    if (fields.region.empty()) {
        return "Invalid s3 conf, empty region";
    }
    ObjCredentialValidationContext context {
            .provider = to_obj_credential_provider(storage_provider(config)),
            .ak = fields.ak,
            .sk = fields.sk,
            .token = fields.token,
            .has_aws_role_arn = !fields.role_arn.empty(),
            .has_aws_external_id = !fields.external_id.empty(),
            .has_aws_credential_provider = fields.cred_provider_type != CredProviderType::Default};
    if (auto error = validate_obj_credential_config(fields.credential, context)) {
        return error;
    }
    if (!context.has_aws_role_arn && fields.ak.empty() != fields.sk.empty()) {
        return fields.ak.empty() ? "Invalid s3 conf, empty ak" : "Invalid s3 conf, empty sk";
    }
    return std::nullopt;
}

bool same_storage_target(const ObjStorageConfig& lhs, const ObjStorageConfig& rhs) {
    const auto& left = storage_config_base(lhs);
    const auto& right = storage_config_base(rhs);
    return lhs.index() == rhs.index() && left.bucket == right.bucket &&
           left.normalized_prefix() == right.normalized_prefix() &&
           left.is_internal_bucket == right.is_internal_bucket && left.endpoint == right.endpoint &&
           left.region == right.region &&
           (std::holds_alternative<AzureConfig>(lhs) ||
            left.need_override_endpoint == right.need_override_endpoint);
}

ObjStorageConfig obj_storage_client_cache_key(const ObjStorageConfig& config) {
    auto key = config;
    auto& fields = storage_config_base(key);
    if (std::holds_alternative<AzureConfig>(key)) {
        fields.role_arn.clear(); // The shared-key SDK client does not consume the role.
    } else {
        fields.bucket.clear();
    }
    fields.prefix.clear();
    fields.sse_enabled = false;
    fields.is_internal_bucket = false;
    return key;
}

size_t ObjStorageClientHash::operator()(const ObjStorageConfig& config) const {
    const auto& fields = storage_config_base(config);
    const std::hash<std::string> hash_string;
    uint64_t hash_code = hash_string(fields.ak + fields.sk);
    hash_code ^= hash_string(fields.endpoint);
    hash_code ^= hash_string(fields.region);
    hash_code ^= static_cast<int>(storage_provider(config));
    hash_code ^= hash_string(fields.bucket);
    hash_combine_credential(&hash_code, fields.credential);
    if (std::holds_alternative<AzureConfig>(config)) {
        return hash_code;
    }
    hash_code ^= hash_string(fields.token);
    hash_code ^= fields.max_connections;
    hash_code ^= fields.request_timeout_ms;
    hash_code ^= fields.connect_timeout_ms;
    hash_code ^= fields.use_virtual_addressing;
    hash_code ^= fields.need_override_endpoint;
    hash_code ^= static_cast<int>(fields.cred_provider_type);
    hash_code ^= hash_string(fields.role_arn);
    hash_code ^= hash_string(fields.external_id);
    return hash_code;
}

std::string obj_storage_config_to_string(const ObjStorageConfig& config) {
    const auto& fields = storage_config_base(config);
    auto description = fmt::format(
            "bucket={}, prefix={}, sse_enabled={}, is_internal_bucket={}, "
            "ak={}, endpoint={}, region={}",
            fields.bucket, fields.prefix, fields.sse_enabled, fields.is_internal_bucket,
            hide_access_key(fields.ak), fields.endpoint, fields.region);
    if (!std::holds_alternative<AzureConfig>(config)) {
        description += fmt::format(
                ", token={}, max_connections={}, request_timeout_ms={}, connect_timeout_ms={}, "
                "use_virtual_addressing={}, cred_provider_type={}, role_arn={}, external_id={}, "
                "credential_type={}",
                fields.token.empty() ? "" : "******", fields.max_connections,
                fields.request_timeout_ms, fields.connect_timeout_ms, fields.use_virtual_addressing,
                static_cast<int>(fields.cred_provider_type), fields.role_arn, fields.external_id,
                credential_type_name(fields.credential));
    }
    return description;
}

std::string hide_access_key(const std::string& ak) {
    std::string key = ak;
    size_t key_len = key.length();
    size_t reserved_count;
    if (key_len > 7) {
        reserved_count = 6;
    } else if (key_len > 2) {
        reserved_count = key_len - 2;
    } else {
        reserved_count = 0;
    }

    size_t x_count = key_len - reserved_count;
    size_t left_x_count = (x_count + 1) / 2;

    if (left_x_count > 0) {
        key.replace(0, left_x_count, left_x_count, 'x');
    }

    if (x_count - left_x_count > 0) {
        key.replace(key_len - (x_count - left_x_count), x_count - left_x_count,
                    x_count - left_x_count, 'x');
    }
    return key;
}

cloud::ObjectStoreInfoPB::Provider vault_storage_provider(const cloud::ObjectStoreInfoPB& info) {
    if (info.has_provider() || info.endpoint().empty()) {
        return info.provider();
    }
    return storage_provider_from_endpoint(info.endpoint());
}

cloud::ObjectStoreInfoPB::Provider storage_provider_from_endpoint(std::string_view endpoint) {
    using Vault = cloud::ObjectStoreInfoPB;
    std::string url(endpoint);
    std::transform(url.begin(), url.end(), url.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    brpc::URI uri;
    if (url.find('\0') != std::string::npos || uri.SetHttpURL(url.c_str()) != 0 ||
        (!uri.scheme().empty() && uri.scheme() != "http" && uri.scheme() != "https")) {
        return Vault::S3;
    }
    std::string_view host = uri.host();
    if (host.ends_with('.')) {
        host.remove_suffix(1);
    }
    if (host.empty() || host.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-.") !=
                                std::string_view::npos) {
        return Vault::S3;
    }

    // Match from the DNS root so virtual-hosted buckets cannot change the service or cloud.
    auto labels = host;
    auto tld = pop_dns_label(labels);
    auto domain = pop_dns_label(labels);
    auto last = pop_dns_label(labels);
    auto previous = pop_dns_label(labels);
    auto is_region = [](std::string_view name) {
        return name.size() > 3 && name[2] == '-' && name[0] >= 'a' && name[0] <= 'z' &&
               name[1] >= 'a' && name[1] <= 'z';
    };
    if (domain == "aliyuncs" && tld == "com") {
        if ((last.starts_with("oss-") && is_region(last.substr(4))) || last == "oss-accelerate" ||
            last == "oss-accelerate-overseas" || (previous == "s3" && is_region(last)) ||
            (last == "oss" && is_region(previous))) {
            return Vault::OSS;
        }
    } else if (domain == "googleapis" && tld == "com" && last == "storage") {
        return Vault::GCP;
    } else if (domain == "myqcloud" && tld == "com" && previous == "cos" && !last.empty()) {
        return Vault::COS;
    } else if (domain == "myhuaweicloud" && tld == "com" && previous == "obs" && !last.empty()) {
        return Vault::OBS;
    } else if (domain == "bcebos" && tld == "com" && !last.empty()) {
        return Vault::BOS;
    } else if ((domain == "volces" || domain == "ivolces") && tld == "com" &&
               ((last.starts_with("tos-") && is_region(last.substr(4))) ||
                (last.starts_with("tos-s3-") && is_region(last.substr(7))))) {
        return Vault::TOS;
    } else if (last == "core" && (previous == "blob" || previous == "dfs") && !labels.empty() &&
               ((domain == "windows" && tld == "net") ||
                (domain == "chinacloudapi" && tld == "cn") ||
                (domain == "usgovcloudapi" && tld == "net") ||
                (domain == "cloudapi" && tld == "de"))) {
        return Vault::AZURE;
    }
    return Vault::S3;
}

std::optional<ObjStorageConfig> make_vault_storage_config(
        cloud::ObjectStoreInfoPB::Provider provider) {
    using Vault = cloud::ObjectStoreInfoPB;
    switch (provider) {
    case Vault::S3:
        return AwsConfig {};
    case Vault::OSS:
        return OssConfig {};
    case Vault::COS:
        return CosConfig {};
    case Vault::BOS:
        return BosConfig {};
    case Vault::OBS:
        return ObsConfig {};
    case Vault::GCP:
        return GcpConfig {};
    case Vault::TOS:
        return TosConfig {};
    case Vault::AZURE:
        return AzureConfig {};
    default:
        return std::nullopt;
    }
}

} // namespace doris
