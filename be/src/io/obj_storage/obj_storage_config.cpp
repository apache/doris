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

#include "io/obj_storage/obj_storage_config.h"

#include <fmt/format.h>

#include "core/string_ref.h"
#include "util/s3_util.h"

namespace doris::io {
namespace {

uint64_t hash_backend(const S3ClientConfig& s3) {
    uint64_t hash_code = 0;
    hash_code ^= static_cast<uint64_t>(s3.vendor);
    hash_code ^= crc32_hash(s3.endpoint);
    hash_code ^= crc32_hash(s3.region);
    hash_code ^= s3.need_override_endpoint;
    hash_code ^= s3.use_virtual_addressing;
    // Hash ak + sk together so that swapping them does not produce the same result.
    hash_code ^= crc32_hash(s3.auth.access_key + s3.auth.secret_key);
    hash_code ^= crc32_hash(s3.auth.session_token);
    hash_code ^= static_cast<uint64_t>(s3.auth.provider_type);
    hash_code ^= crc32_hash(s3.auth.role_arn);
    hash_code ^= crc32_hash(s3.auth.external_id);
    return hash_code;
}

uint64_t hash_azure_auth(const std::string& account, const AzureSharedKeyConfig& shared_key) {
    // Hash account + key together, as ak + sk for S3.
    return crc32_hash(account + shared_key.account_key);
}

uint64_t hash_backend(const AzureClientConfig& azure) {
    uint64_t hash_code = 0;
    hash_code ^= static_cast<uint64_t>(ObjStorageProvider::AZURE);
    hash_code ^= crc32_hash(azure.endpoint);
    hash_code ^= crc32_hash(azure.container);
    hash_code ^= std::visit([&](const auto& auth) { return hash_azure_auth(azure.account, auth); },
                            azure.auth);
    return hash_code;
}

std::string backend_to_string(const S3ClientConfig& s3) {
    return fmt::format(
            "s3(vendor={}, endpoint={}, region={}, need_override_endpoint={}, "
            "use_virtual_addressing={}, ak={}, token={}, cred_provider_type={}, role_arn={}, "
            "external_id={})",
            static_cast<int>(s3.vendor), s3.endpoint, s3.region, s3.need_override_endpoint,
            s3.use_virtual_addressing, hide_access_key(s3.auth.access_key),
            s3.auth.session_token.empty() ? "" : "******", static_cast<int>(s3.auth.provider_type),
            s3.auth.role_arn, s3.auth.external_id);
}

const char* azure_auth_name(const AzureSharedKeyConfig&) {
    return "shared_key";
}

std::string backend_to_string(const AzureClientConfig& azure) {
    auto auth = std::visit([](const auto& a) { return azure_auth_name(a); }, azure.auth);
    return fmt::format("azure(endpoint={}, account={}, container={}, auth={})", azure.endpoint,
                       azure.account, azure.container, auth);
}

ObjStorageProvider backend_provider(const S3ClientConfig& s3) {
    return s3.vendor;
}

ObjStorageProvider backend_provider(const AzureClientConfig&) {
    return ObjStorageProvider::AZURE;
}

} // namespace

uint64_t ObjClientConfig::hash() const {
    uint64_t hash_code = std::visit([](const auto& b) { return hash_backend(b); }, backend);
    hash_code ^= connection.max_connections;
    hash_code ^= connection.request_timeout_ms;
    hash_code ^= connection.connect_timeout_ms;
    hash_code ^= static_cast<uint64_t>(usage);
    return hash_code;
}

ObjStorageProvider ObjClientConfig::provider() const {
    return std::visit([](const auto& b) { return backend_provider(b); }, backend);
}

const std::string& ObjClientConfig::endpoint() const {
    return std::visit([](const auto& b) -> const std::string& { return b.endpoint; }, backend);
}

std::string ObjClientConfig::to_string() const {
    return fmt::format(
            "({}, max_connections={}, request_timeout_ms={}, connect_timeout_ms={}, usage={})",
            std::visit([](const auto& b) { return backend_to_string(b); }, backend),
            connection.max_connections, connection.request_timeout_ms,
            connection.connect_timeout_ms,
            usage == ClientUsage::INTERNAL_STORAGE ? "internal_storage" : "external_access");
}

} // namespace doris::io
