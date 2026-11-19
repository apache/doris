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

#include "io/object_storage/be_obj_storage_config_parser.h"

#include <aws/core/client/ClientConfiguration.h>

#include <chrono>
#include <type_traits>
#include <utility>

#include "common/config.h"
#include "cpp/aws_sdk.h"
#include "cpp/obj-storage/auth/ca_bundle_cache.h"
#include "cpp/obj-storage/parser/obj_storage_config_parser.h"

namespace doris {
namespace {
void set_sts_timeout(std::optional<AwsAssumeRole>& role) {
    if (role && !role->request_timeout) {
        role->request_timeout = std::chrono::milliseconds(config::aws_client_request_timeout_ms);
    }
}

Result<ObjStorageResourceConfig> finish_be_obj_storage_config(
        ObjStorageConfigParseResult<ObjStorageResourceConfig> parsed) {
    if (auto* error = std::get_if<std::string>(&parsed)) {
        return ResultError(Status::InvalidArgument<false>(*error));
    }
    auto resource = std::get<ObjStorageResourceConfig>(std::move(parsed));
    // The old BE filesystem ignored this metadata flag. Built-in MinIO vaults can
    // carry sse_enabled=true without KMS, so forwarding it would break existing writes.
    resource.sse_enabled = false;
    apply_be_obj_storage_defaults(resource.client);
    return resource;
}

} // namespace

Result<ObjStorageResourceConfig> parse_be_obj_storage_config(
        const std::map<std::string, std::string>& properties, std::string bucket) {
    return finish_be_obj_storage_config(ObjStorageConfigParser::from_properties(
            properties, std::move(bucket),
            aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS));
}

Result<ObjStorageResourceConfig> parse_be_obj_storage_config(const TS3StorageParam& param) {
    return finish_be_obj_storage_config(ObjStorageConfigParser::from_storage_param(
            param, aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS));
}

Result<ObjStorageResourceConfig> parse_be_obj_storage_config(const cloud::ObjectStoreInfoPB& info) {
    return finish_be_obj_storage_config(ObjStorageConfigParser::from_vault(
            info, aws_credential_implementation(config::aws_credentials_provider_version),
            EmptyCredentialsBehavior::ANONYMOUS));
}

void apply_be_obj_storage_defaults(ObjStorageClientConfig& client) {
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
                    }
                }
            },
            client);
}

std::string be_ca_bundle_path() {
    static CaBundleCache cache;
    return cache.path(config::ca_cert_file_paths);
}

Aws::Client::ClientConfiguration be_default_aws_client_configuration() {
    ensure_aws_sdk_initialized(config::aws_log_level);
    auto defaults = default_aws_client_configuration();
    defaults.requestTimeoutMs = config::aws_client_request_timeout_ms;
    return defaults;
}

} // namespace doris
