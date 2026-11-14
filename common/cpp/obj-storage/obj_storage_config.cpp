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

std::string_view ObjStorageResourceConfig::normalized_prefix() const {
    const auto first = prefix.find_first_not_of('/');
    if (first == std::string::npos) {
        return {};
    }
    return std::string_view(prefix).substr(first, prefix.find_last_not_of('/') - first + 1);
}

namespace {
using Error = std::optional<std::string>;
using Milliseconds = std::chrono::milliseconds;

Error validate_connection(const ObjStorageConnectionConfig& connection, bool require_endpoint) {
    if (require_endpoint && connection.endpoint.empty()) {
        return "empty object storage endpoint";
    }
    if (connection.max_connections && *connection.max_connections <= 0) {
        return "max_connections must be positive";
    }
    if (connection.max_retries && *connection.max_retries < 0) {
        return "max_retries must be nonnegative";
    }
    if (connection.connect_timeout && *connection.connect_timeout < Milliseconds::zero()) {
        return "negative connect timeout";
    }
    if (connection.request_timeout && *connection.request_timeout < Milliseconds::zero()) {
        return "negative request timeout";
    }
    return {};
}

Error validate_role(const std::optional<AwsAssumeRole>& role) {
    if (role && (role->role_arn.empty() ||
                 (role->request_timeout && *role->request_timeout < Milliseconds::zero()))) {
        return "invalid AssumeRole configuration";
    }
    return {};
}

Error validate_auth(const AccessKeyObjStorageAuth& keys) {
    if (keys.access_key.empty() || keys.secret_key.empty()) {
        return "incomplete access key credentials";
    }
    return {};
}
Error validate_auth(const AnonymousObjStorageAuth&) {
    return {};
}
Error validate_auth(AwsCredentialChainType) {
    return {};
}
Error validate_auth(const S3CredentialChain& chain) {
    return validate_role(chain.assume_role);
}
Error validate_auth(const GcpHmacObjStorageAuth& hmac) {
    if (hmac.access_id.empty() || hmac.secret_key.empty()) {
        return "incomplete GCP HMAC credentials";
    }
    return {};
}
Error validate_auth(const GcpNativeObjStorageAuth& auth) {
    if (auth.impersonation_service_account &&
        !is_valid_gcp_service_account_email(*auth.impersonation_service_account)) {
        return "Invalid GCP service account email";
    }
    return {};
}
template <class... T>
Error validate_auth(const std::variant<T...>& auth) {
    return std::visit([](const auto& selected) { return validate_auth(selected); }, auth);
}
Error validate_auth(const AwsObjStorageAuthConfig& auth) {
    if (auto error = validate_role(auth.assume_role)) {
        return error;
    }
    return validate_auth(auth.source);
}
template <class C>
Error validate_regional_client(const C& config, bool require_endpoint = true) {
    if (auto error = validate_connection(config.connection, require_endpoint)) {
        return error;
    }
    if (config.region.empty()) {
        return "empty object storage region";
    }
    return validate_auth(config.auth);
}

// Hash the effective settings in field order. Equality of the variant remains
// authoritative even when two configurations fall in the same hash bucket.
template <class T>
void combine(size_t& seed, const T& input) {
    seed ^= std::hash<T> {}(input) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}
void combine(size_t& seed, const std::optional<Milliseconds>& duration) {
    if (duration) {
        combine(seed, duration->count());
    }
    combine(seed, duration.has_value());
}
template <class... Inputs>
size_t fields(const Inputs&... inputs) {
    size_t seed = 0;
    (combine(seed, inputs), ...);
    return seed;
}
size_t auth_hash(const AnonymousObjStorageAuth&) {
    return 0;
}
size_t auth_hash(AwsCredentialChainType chain) {
    return fields(chain);
}
size_t auth_hash(const AccessKeyObjStorageAuth& keys) {
    return fields(keys.access_key, keys.secret_key, keys.session_token);
}
size_t auth_hash(const GcpHmacObjStorageAuth& keys) {
    return fields(keys.access_id, keys.secret_key, keys.session_token);
}
size_t auth_hash(const AzureObjStorageAuthConfig& auth) {
    return fields(auth.account_name, auth.account_key);
}
size_t auth_hash(const GcpNativeObjStorageAuth& auth) {
    return fields(auth.source, auth.impersonation_service_account);
}
size_t role_hash(const std::optional<AwsAssumeRole>& role) {
    return role ? fields(role->role_arn, role->external_id, role->request_timeout) : 0;
}
template <class... T>
size_t auth_hash(const std::variant<T...>& auth);
size_t auth_hash(const S3CredentialChain& chain) {
    return fields(auth_hash(chain.source), chain.implementation, role_hash(chain.assume_role));
}
size_t auth_hash(const AwsObjStorageAuthConfig& aws) {
    return fields(auth_hash(aws.source), aws.implementation, role_hash(aws.assume_role));
}
template <class... T>
size_t auth_hash(const std::variant<T...>& auth) {
    return std::visit([&](const auto& value) { return fields(auth.index(), auth_hash(value)); },
                      auth);
}
} // namespace

Error AwsObjStorageConfig::validate() const {
    return validate_regional_client(*this, !use_sdk_endpoint);
}
Error OssObjStorageConfig::validate() const {
    return validate_regional_client(*this);
}
Error CosObjStorageConfig::validate() const {
    return validate_regional_client(*this);
}
Error BosObjStorageConfig::validate() const {
    return validate_regional_client(*this);
}
Error ObsObjStorageConfig::validate() const {
    return validate_regional_client(*this);
}
Error TosObjStorageConfig::validate() const {
    return validate_regional_client(*this);
}
Error GcpObjStorageConfig::validate() const {
    return validate_regional_client(*this);
}

Error AzureObjStorageConfig::validate() const {
    if (auto error = validate_connection(connection, true)) {
        return error;
    }
    if (connection.max_connections) {
        return "Azure does not expose a connection pool size";
    }
    if (auth.account_name.empty() || auth.account_key.empty()) {
        return "incomplete Azure shared-key credentials";
    }
    return {};
}

ObjStorageConnectionConfig& obj_storage_connection(ObjStorageClientConfig& config) {
    return std::visit([](auto& client) -> ObjStorageConnectionConfig& { return client.connection; },
                      config);
}
const ObjStorageConnectionConfig& obj_storage_connection(const ObjStorageClientConfig& config) {
    return std::visit(
            [](const auto& client) -> const ObjStorageConnectionConfig& {
                return client.connection;
            },
            config);
}
ObjStorageProvider storage_provider(const ObjStorageClientConfig& config) {
    return std::visit([](const auto& client) { return client.provider; }, config);
}
Error validate_obj_storage_config(const ObjStorageClientConfig& config) {
    return std::visit([](const auto& client) { return client.validate(); }, config);
}
bool same_client_target(const ObjStorageClientConfig& lhs, const ObjStorageClientConfig& rhs) {
    return std::visit(
            [&]<class C>(const C& client) {
                const auto* next = std::get_if<C>(&rhs);
                if (!next || client.connection.endpoint != next->connection.endpoint) {
                    return false;
                }
                if constexpr (std::is_same_v<C, AzureObjStorageConfig>) {
                    return client.auth.account_name == next->auth.account_name;
                } else if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
                    return client.region == next->region &&
                           client.use_sdk_endpoint == next->use_sdk_endpoint &&
                           client.sdk_endpoint_http == next->sdk_endpoint_http;
                } else {
                    return client.region == next->region;
                }
            },
            lhs);
}
size_t ObjStorageClientHash::operator()(const ObjStorageClientConfig& config) const {
    return std::visit(
            []<class C>(const C& client) {
                const auto& conn = client.connection;
                size_t seed =
                        fields(client.provider, conn.endpoint, conn.ca_cert_file,
                               conn.max_connections, conn.connect_timeout, conn.request_timeout,
                               conn.max_retries, auth_hash(client.auth));
                if constexpr (std::is_same_v<C, AzureObjStorageConfig>) {
                    combine(seed, client.retry_too_many_requests);
                } else {
                    combine(seed,
                            fields(client.region, client.use_path_style, client.retry_slow_down));
                    if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
                        combine(seed, fields(client.use_sdk_endpoint, client.sdk_endpoint_http));
                    }
                }
                return seed;
            },
            config);
}
std::string obj_storage_config_to_string(const ObjStorageClientConfig& config) {
    return fmt::format("provider={}, endpoint={}", static_cast<int>(storage_provider(config)),
                       obj_storage_connection(config).endpoint);
}

} // namespace doris
