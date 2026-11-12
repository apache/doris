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

#include <fmt/format.h>

#include <functional>
#include <type_traits>

#include "cpp/obj-storage/auth/gcp/gcp_auth.h"

namespace doris {
namespace {
using Error = std::optional<std::string>;

Error validate_connection(const ObjStorageConnectionConfig& connection, bool require_endpoint) {
    if (require_endpoint && connection.endpoint.empty()) {
        return "Invalid object storage config, empty endpoint";
    }
    if ((connection.max_connections && *connection.max_connections <= 0) ||
        (connection.max_retries && *connection.max_retries < 0) ||
        (connection.request_timeout && connection.request_timeout->count() < 0) ||
        (connection.connect_timeout && connection.connect_timeout->count() < 0)) {
        return "Invalid object storage connection limits or timeout";
    }
    return std::nullopt;
}

Error validate_auth(const AnonymousObjStorageAuth&) {
    return std::nullopt;
}
Error validate_auth(const ObjStorageCredentialChain&) {
    return std::nullopt;
}
Error validate_auth(AwsCredentialChainType) {
    return std::nullopt;
}
Error validate_auth(const AccessKeyObjStorageAuth& auth) {
    if (auth.access_key.empty() || auth.secret_key.empty()) {
        return "Incomplete access key credentials";
    }
    return std::nullopt;
}
Error validate_auth(const GcpHmacObjStorageAuth& auth) {
    if (auth.access_id.empty() || auth.secret_key.empty()) {
        return "Incomplete GCP HMAC credentials";
    }
    return std::nullopt;
}
Error validate_auth(const GcpNativeObjStorageAuth& auth) {
    if (auth.impersonation_service_account &&
        !is_valid_gcp_service_account_email(*auth.impersonation_service_account)) {
        return "Invalid GCP service account email";
    }
    return std::nullopt;
}
template <typename... T>
Error validate_auth(const std::variant<T...>& auth) {
    return std::visit([](const auto& selected) { return validate_auth(selected); }, auth);
}
Error validate_auth(const AwsObjStorageAuthConfig& auth) {
    if (auth.assume_role &&
        (auth.assume_role->role_arn.empty() ||
         (auth.assume_role->request_timeout && auth.assume_role->request_timeout->count() < 0))) {
        return "Invalid AWS AssumeRole configuration";
    }
    return validate_auth(auth.source);
}
template <typename Config>
Error validate_region_client(const Config& config, bool require_endpoint = true) {
    if (auto error = validate_connection(config.connection, require_endpoint)) {
        return error;
    }
    if (config.region.empty()) {
        return "Invalid object storage config, empty region";
    }
    return validate_auth(config.auth);
}

// Equal configs include every value used for SDK creation, including the local
// CA file and retry policy. Distinguish absent options from explicit zeroes.
template <typename T>
void hash_value(size_t& hash, const T& v) {
    hash ^= std::hash<T> {}(v) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
}
void hash_value(size_t& hash, std::chrono::milliseconds v) {
    hash_value(hash, v.count());
}
template <typename T>
void hash_value(size_t& hash, const std::optional<T>& v) {
    hash_value(hash, v.has_value());
    if (v) {
        hash_value(hash, *v);
    }
}
void hash_auth(size_t&, const AnonymousObjStorageAuth&) {}
void hash_auth(size_t& h, AwsCredentialChainType value) {
    hash_value(h, value);
}
void hash_auth(size_t& h, const ObjStorageCredentialChain& a) {
    hash_value(h, a.type);
    hash_value(h, a.implementation);
}
void hash_auth(size_t& h, const AccessKeyObjStorageAuth& a) {
    hash_value(h, a.access_key);
    hash_value(h, a.secret_key);
    hash_value(h, a.session_token);
}
void hash_auth(size_t& h, const GcpHmacObjStorageAuth& a) {
    hash_value(h, a.access_id);
    hash_value(h, a.secret_key);
    hash_value(h, a.session_token);
}
void hash_auth(size_t& h, const GcpNativeObjStorageAuth& a) {
    hash_value(h, a.source);
    hash_value(h, a.impersonation_service_account);
}
void hash_auth(size_t& h, const AzureObjStorageAuthConfig& a) {
    hash_value(h, a.account_name);
    hash_value(h, a.account_key);
}
template <typename... T>
void hash_auth(size_t& h, const std::variant<T...>& a) {
    hash_value(h, a.index());
    std::visit([&](const auto& selected) { hash_auth(h, selected); }, a);
}
void hash_auth(size_t& h, const AwsObjStorageAuthConfig& a) {
    hash_auth(h, a.source);
    hash_value(h, a.implementation);
    hash_value(h, a.assume_role.has_value());
    if (a.assume_role) {
        hash_value(h, a.assume_role->role_arn);
        hash_value(h, a.assume_role->external_id);
        hash_value(h, a.assume_role->request_timeout);
    }
}
} // namespace
Error AwsObjStorageConfig::validate() const {
    return validate_region_client(*this, !use_sdk_endpoint);
}
Error OssObjStorageConfig::validate() const {
    return validate_region_client(*this);
}
Error CosObjStorageConfig::validate() const {
    return validate_region_client(*this);
}
Error BosObjStorageConfig::validate() const {
    return validate_region_client(*this);
}
Error ObsObjStorageConfig::validate() const {
    return validate_region_client(*this);
}
Error TosObjStorageConfig::validate() const {
    return validate_region_client(*this);
}
Error GcpObjStorageConfig::validate() const {
    return validate_region_client(*this);
}
Error AzureObjStorageConfig::validate() const {
    if (auto error = validate_connection(connection, true)) {
        return error;
    }
    if (connection.max_connections) {
        return "Azure Blob client does not support a connection pool limit";
    }
    if (auth.account_name.empty() || auth.account_key.empty()) {
        return "Incomplete Azure shared key credentials";
    }
    return std::nullopt;
}
std::string_view ObjStorageResourceConfig::normalized_prefix() const {
    const auto first = prefix.find_first_not_of('/');
    if (first == std::string::npos) {
        return {};
    }
    return std::string_view(prefix).substr(first, prefix.find_last_not_of('/') - first + 1);
}
const ObjStorageConnectionConfig& obj_storage_connection(const ObjStorageClientConfig& config) {
    return std::visit([](const auto& c) -> const auto& { return c.connection; }, config);
}
ObjStorageConnectionConfig& obj_storage_connection(ObjStorageClientConfig& config) {
    return std::visit([](auto& c) -> auto& { return c.connection; }, config);
}
ObjStorageProvider storage_provider(const ObjStorageClientConfig& config) {
    return std::visit(
            []<typename C>(const C&) {
                if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
                    return ObjStorageProvider::AWS;
                } else if constexpr (std::is_same_v<C, OssObjStorageConfig>) {
                    return ObjStorageProvider::OSS;
                } else if constexpr (std::is_same_v<C, CosObjStorageConfig>) {
                    return ObjStorageProvider::COS;
                } else if constexpr (std::is_same_v<C, BosObjStorageConfig>) {
                    return ObjStorageProvider::BOS;
                } else if constexpr (std::is_same_v<C, ObsObjStorageConfig>) {
                    return ObjStorageProvider::OBS;
                } else if constexpr (std::is_same_v<C, TosObjStorageConfig>) {
                    return ObjStorageProvider::TOS;
                } else if constexpr (std::is_same_v<C, GcpObjStorageConfig>) {
                    return ObjStorageProvider::GCP;
                } else if constexpr (std::is_same_v<C, AzureObjStorageConfig>) {
                    return ObjStorageProvider::AZURE;
                }
            },
            config);
}
Error validate_obj_storage_config(const ObjStorageClientConfig& config) {
    return std::visit([](const auto& c) { return c.validate(); }, config);
}
bool same_client_target(const ObjStorageClientConfig& lhs, const ObjStorageClientConfig& rhs) {
    return std::visit(
            [&]<typename C>(const C& left) {
                const auto* right = std::get_if<C>(&rhs);
                if (!right || left.connection.endpoint != right->connection.endpoint) {
                    return false;
                }
                if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
                    if (left.use_sdk_endpoint != right->use_sdk_endpoint ||
                        left.sdk_endpoint_http != right->sdk_endpoint_http) {
                        return false;
                    }
                }
                if constexpr (std::is_same_v<C, AzureObjStorageConfig>) {
                    return left.auth.account_name == right->auth.account_name;
                } else {
                    return left.region == right->region;
                }
            },
            lhs);
}
size_t ObjStorageClientHash::operator()(const ObjStorageClientConfig& config) const {
    size_t h = config.index();
    std::visit(
            [&]<typename C>(const C& provider) {
                const auto& c = provider.connection;
                hash_value(h, c.endpoint);
                hash_value(h, c.ca_cert_file);
                hash_value(h, c.max_connections);
                hash_value(h, c.connect_timeout);
                hash_value(h, c.request_timeout);
                hash_value(h, c.max_retries);
                hash_auth(h, provider.auth);
                if constexpr (std::is_same_v<C, AzureObjStorageConfig>) {
                    hash_value(h, provider.retry_too_many_requests);
                } else {
                    hash_value(h, provider.region);
                    hash_value(h, provider.use_path_style);
                    hash_value(h, provider.retry_slow_down);
                }
                if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
                    hash_value(h, provider.use_sdk_endpoint);
                    hash_value(h, provider.sdk_endpoint_http);
                }
            },
            config);
    return h;
}
std::string obj_storage_config_to_string(const ObjStorageClientConfig& config) {
    return fmt::format("provider={}, endpoint={}", static_cast<int>(storage_provider(config)),
                       obj_storage_connection(config).endpoint);
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

} // namespace doris
