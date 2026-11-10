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

#include <string_view>
#include <variant>

#include "core/string_ref.h"
#include "util/s3_util.h"

namespace doris::io {
namespace {

uint64_t hash_backend(const S3ClientConfig& s3) {
    uint64_t hash_code = 0;
    hash_code ^= static_cast<uint64_t>(s3.vendor);
    hash_code ^= crc32_hash(s3.endpoint);
    hash_code ^= crc32_hash(s3.region);
    hash_code ^= crc32_hash(s3.cache_bucket);
    hash_code ^= s3.need_override_endpoint;
    hash_code ^= s3.use_virtual_addressing;
    // Hash ak + sk together so that swapping them does not produce the same result.
    hash_code ^= crc32_hash(s3.auth.access_key + s3.auth.secret_key);
    hash_code ^= crc32_hash(s3.auth.session_token);
    hash_code ^= static_cast<uint64_t>(s3.auth.provider_type);
    hash_code ^= crc32_hash(s3.auth.role_arn);
    hash_code ^= crc32_hash(s3.auth.external_id);
    hash_combine_credential(&hash_code, s3.auth.credential);
    return hash_code;
}

uint64_t hash_backend(const AzureClientConfig& azure) {
    uint64_t hash_code = 0;
    hash_code ^= static_cast<uint64_t>(ObjStorageProvider::AZURE);
    hash_code ^= crc32_hash(azure.endpoint);
    hash_code ^= crc32_hash(azure.container);
    hash_combine_azure_auth(&hash_code, azure.account, azure.auth);
    if (azure.legacy) {
        const auto& fields = *azure.legacy;
        hash_code ^= crc32_hash(fields.region);
        hash_code ^= fields.need_override_endpoint;
        hash_code ^= fields.use_virtual_addressing;
        hash_code ^= crc32_hash(fields.session_token);
        hash_code ^= static_cast<uint64_t>(fields.provider_type);
        hash_code ^= crc32_hash(fields.role_arn);
        hash_code ^= crc32_hash(fields.external_id);
        hash_combine_credential(&hash_code, fields.credential);
    }
    return hash_code;
}

std::string backend_to_string(const S3ClientConfig& s3) {
    return fmt::format(
            "s3(vendor={}, endpoint={}, region={}, need_override_endpoint={}, "
            "use_virtual_addressing={}, ak={}, token={}, cred_provider_type={}, role_arn={}, "
            "external_id={}, credential_type={})",
            static_cast<int>(s3.vendor), s3.endpoint, s3.region, s3.need_override_endpoint,
            s3.use_virtual_addressing, hide_access_key(s3.auth.access_key),
            s3.auth.session_token.empty() ? "" : "******", static_cast<int>(s3.auth.provider_type),
            s3.auth.role_arn, s3.auth.external_id, credential_type_name(s3.auth.credential));
}

std::string backend_to_string(const AzureClientConfig& azure) {
    return fmt::format("azure(endpoint={}, account={}, container={}, auth={})", azure.endpoint,
                       azure.account, azure.container, azure_auth_type_name(azure.auth));
}

ObjStorageProvider backend_provider(const S3ClientConfig& s3) {
    return s3.vendor;
}

ObjStorageProvider backend_provider(const AzureClientConfig&) {
    return ObjStorageProvider::AZURE;
}

// Adapter from the storage-domain provider to the credential layer's own provider identity,
// used where a provider-native credential is validated.
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

// Validate the effective flat input, after a refresh has retained its original target.
Status validate_flat_client(std::string_view endpoint, std::string_view region,
                            const CredentialConfig& credential,
                            const ObjCredentialValidationContext& auth) {
    if (endpoint.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty endpoint");
    }
    if (region.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty region");
    }
    if (auto error = validate_obj_credential_config(credential, auth); error.has_value()) {
        return Status::InvalidArgument<false>(*error);
    }
    if (!auth.has_aws_role_arn) {
        if (!auth.ak.empty() && auth.sk.empty()) {
            return Status::InvalidArgument<false>("Invalid s3 conf, empty sk");
        }
        if (!auth.sk.empty() && auth.ak.empty()) {
            return Status::InvalidArgument<false>("Invalid s3 conf, empty ak");
        }
    }
    return Status::OK();
}

Status validate_client_backend(const S3ClientConfig& s3) {
    return validate_flat_client(
            s3.endpoint, s3.region, s3.auth.credential,
            {.provider = to_obj_credential_provider(s3.vendor),
             .ak = s3.auth.access_key,
             .sk = s3.auth.secret_key,
             .token = s3.auth.session_token,
             .has_aws_role_arn = !s3.auth.role_arn.empty(),
             .has_aws_external_id = !s3.auth.external_id.empty(),
             .has_aws_credential_provider = s3.auth.provider_type != CredProviderType::Default});
}

Status validate_client_backend(const AzureClientConfig& azure) {
    if (azure.legacy) {
        const auto& fields = *azure.legacy;
        return validate_flat_client(
                azure.endpoint, fields.region, fields.credential,
                {.provider = ObjCredentialProvider::Azure,
                 .ak = azure.account,
                 .sk = azure_auth_account_key(azure.auth),
                 .token = fields.session_token,
                 .has_aws_role_arn = !fields.role_arn.empty(),
                 .has_aws_external_id = !fields.external_id.empty(),
                 .has_aws_credential_provider = fields.provider_type != CredProviderType::Default});
    }
    if (azure.endpoint.empty()) {
        return Status::InvalidArgument<false>("Invalid azure conf, empty endpoint");
    }
    if (auto error = validate_azure_auth(azure.account, azure.auth); error.has_value()) {
        return Status::InvalidArgument<false>(*error);
    }
    return Status::OK();
}

// Native Azure requests are opened directly and carry no legacy region or endpoint override.
template <typename Config, typename Visitor>
void visit_flat_target(Config& config, Visitor visit) {
    if (auto* s3 = std::get_if<S3ClientConfig>(&config.backend)) {
        visit(*s3);
    } else if (auto& fields = std::get<AzureClientConfig>(config.backend).legacy; fields) {
        visit(*fields);
    }
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

bool ObjClientConfig::has_role_arn() const {
    if (const auto* s3 = std::get_if<S3ClientConfig>(&backend)) {
        return !s3->auth.role_arn.empty();
    }
    const auto& fields = std::get<AzureClientConfig>(backend).legacy;
    return fields && !fields->role_arn.empty();
}

std::string ObjClientConfig::to_string() const {
    return fmt::format(
            "({}, max_connections={}, request_timeout_ms={}, connect_timeout_ms={}, usage={})",
            std::visit([](const auto& b) { return backend_to_string(b); }, backend),
            connection.max_connections, connection.request_timeout_ms,
            connection.connect_timeout_ms,
            usage == ClientUsage::INTERNAL_STORAGE ? "internal_storage" : "external_access");
}

std::string ObjStorageConfig::to_string() const {
    return fmt::format("(bucket={}, prefix={}, client={}, sse_enabled={})", bucket, prefix,
                       client.to_string(), sse_enabled);
}

Status validate_obj_client_config(const ObjClientConfig& config) {
    return std::visit([](const auto& backend) { return validate_client_backend(backend); },
                      config.backend);
}

ObjClientConfig merge_obj_client_config(const ObjClientConfig& current, ObjClientConfig update) {
    std::visit([&](auto& backend) { backend.endpoint = current.endpoint(); }, update.backend);
    visit_flat_target(current, [&](const auto& original) {
        visit_flat_target(update, [&](auto& next) {
            next.region = original.region;
            next.need_override_endpoint = original.need_override_endpoint;
        });
    });
    return update;
}

} // namespace doris::io
