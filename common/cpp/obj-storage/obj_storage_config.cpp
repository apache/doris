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
#include <type_traits>
#include <utility>

namespace doris {
namespace {

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

std::optional<std::string> GcpConfig::validate_credential(std::string_view aws_role_arn,
                                                          std::string_view aws_external_id) const {
    if (!credential) {
        return std::nullopt;
    }
    return validate_gcp_credential(
            *credential,
            {
                    .provider = ObjCredentialProvider::Gcp,
                    .ak = ak,
                    .sk = sk,
                    .token = token,
                    .has_aws_role_arn = !aws_role_arn.empty(),
                    .has_aws_external_id = !aws_external_id.empty(),
                    .has_aws_credential_provider = cred_provider_type != CredProviderType::Default,
            });
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
    if (auto* gcp = std::get_if<GcpConfig>(&config)) {
        if (auto error = gcp->validate_credential()) {
            return error;
        }
    }
    const auto* aws = std::get_if<AwsConfig>(&config);
    if ((!aws || aws->role_arn.empty()) && fields.ak.empty() != fields.sk.empty()) {
        return fields.ak.empty() ? "Invalid s3 conf, empty ak" : "Invalid s3 conf, empty sk";
    }
    return std::nullopt;
}

bool same_storage_target(const ObjStorageConfig& lhs, const ObjStorageConfig& rhs) {
    return std::visit(
            [&](const auto& left) {
                using Config = std::decay_t<decltype(left)>;
                const auto* right = std::get_if<Config>(&rhs);
                if (!right || left.bucket != right->bucket ||
                    left.normalized_prefix() != right->normalized_prefix() ||
                    left.is_internal_bucket != right->is_internal_bucket ||
                    left.endpoint != right->endpoint || left.region != right->region) {
                    return false;
                }
                if constexpr (std::is_base_of_v<S3CompatibleConfig, Config>) {
                    return left.need_override_endpoint == right->need_override_endpoint;
                }
                return true;
            },
            lhs);
}

ObjStorageConfig obj_storage_client_cache_key(const ObjStorageConfig& config) {
    auto key = config;
    auto& fields = storage_config_base(key);
    if (!std::holds_alternative<AzureConfig>(key)) {
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
    std::visit(
            [&](const auto& provider) {
                using Config = std::decay_t<decltype(provider)>;
                if constexpr (std::is_base_of_v<S3CompatibleConfig, Config>) {
                    hash_code ^= static_cast<int>(provider.cred_provider_type);
                    hash_code ^= hash_string(provider.token);
                    hash_code ^= provider.max_connections;
                    hash_code ^= provider.request_timeout_ms;
                    hash_code ^= provider.connect_timeout_ms;
                    hash_code ^= provider.use_virtual_addressing;
                    hash_code ^= provider.need_override_endpoint;
                }
                if constexpr (std::is_same_v<Config, AwsConfig>) {
                    hash_code ^= hash_string(provider.role_arn);
                    hash_code ^= hash_string(provider.external_id);
                } else if constexpr (std::is_same_v<Config, GcpConfig>) {
                    if (provider.credential) {
                        hash_code ^= static_cast<size_t>(provider.credential->provider_type) + 1;
                        hash_code ^=
                                hash_string(provider.credential->impersonation_service_account);
                    }
                }
            },
            config);
    return hash_code;
}

std::string obj_storage_config_to_string(const ObjStorageConfig& config) {
    const auto& fields = storage_config_base(config);
    auto description = fmt::format(
            "bucket={}, prefix={}, sse_enabled={}, is_internal_bucket={}, "
            "ak={}, endpoint={}, region={}",
            fields.bucket, fields.prefix, fields.sse_enabled, fields.is_internal_bucket,
            hide_access_key(fields.ak), fields.endpoint, fields.region);
    std::visit(
            [&](const auto& provider) {
                using Config = std::decay_t<decltype(provider)>;
                if constexpr (std::is_base_of_v<S3CompatibleConfig, Config>) {
                    description += fmt::format(
                            ", token={}, max_connections={}, request_timeout_ms={}, "
                            "connect_timeout_ms={}, "
                            "use_virtual_addressing={}, cred_provider_type={}",
                            provider.token.empty() ? "" : "******", provider.max_connections,
                            provider.request_timeout_ms, provider.connect_timeout_ms,
                            provider.use_virtual_addressing,
                            static_cast<int>(provider.cred_provider_type));
                }
                if constexpr (std::is_same_v<Config, AwsConfig>) {
                    description += fmt::format(", role_arn={}, external_id={}", provider.role_arn,
                                               provider.external_id);
                } else if constexpr (std::is_same_v<Config, GcpConfig>) {
                    description += fmt::format(", credential_type={}",
                                               provider.credential ? "gcp" : "none");
                }
            },
            config);
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

namespace {

using VaultInfo = cloud::ObjectStoreInfoPB;

void parse_vault_location(const VaultInfo& info, ObjStorageConfigBase& config) {
    config.bucket = info.bucket();
    config.prefix = info.prefix();
    config.sse_enabled = info.sse_enabled();
    config.is_internal_bucket = true;
    config.endpoint = info.endpoint();
    config.region = info.region();
    config.ak = info.ak();
    config.sk = info.sk();
}

std::optional<std::string> parse_vault(const VaultInfo& info, AzureConfig& config) {
    parse_vault_location(info, config);
    return std::nullopt;
}

void parse_s3_vault(const VaultInfo& info, S3CompatibleConfig& config) {
    parse_vault_location(info, config);
    config.use_virtual_addressing = !info.use_path_style();
    if (info.has_cred_provider_type()) {
        config.cred_provider_type = cred_provider_type_from_pb(info.cred_provider_type());
    }
}

std::optional<std::string> parse_vault(const VaultInfo& info, S3CompatibleConfig& config) {
    parse_s3_vault(info, config);
    return std::nullopt;
}

std::optional<std::string> parse_vault(const VaultInfo& info, AwsConfig& config) {
    parse_s3_vault(info, config);
    config.role_arn = info.role_arn();
    config.external_id = info.external_id();
    return std::nullopt;
}

std::optional<std::string> parse_vault(const VaultInfo& info, GcpConfig& config) {
    parse_s3_vault(info, config);
    if (!info.credential().has_gcp_credential()) {
        return std::nullopt;
    }
    config.credential = to_gcp_credential_config(info.credential().gcp_credential());
    return config.validate_credential(info.role_arn(), info.external_id());
}

} // namespace

std::optional<std::string> parse_vault_storage_config(const cloud::ObjectStoreInfoPB& info,
                                                      ObjStorageConfig* result) {
    const auto provider = vault_storage_provider(info);
    if (provider != VaultInfo::GCP && info.credential().has_gcp_credential()) {
        return "GCP credentials require provider=GCP";
    }
    return parse_storage_provider(provider, [&](auto config) -> std::optional<std::string> {
        if (auto error = parse_vault(info, config)) {
            return error;
        }
        *result = std::move(config);
        return std::nullopt;
    });
}

} // namespace doris
