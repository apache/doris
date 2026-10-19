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

#include <string_view>
#include <variant>

#include "io/obj_storage/azure_config_codec.h"
#include "io/obj_storage/azure_config_validator.h"
#include "io/obj_storage/obj_storage_uri.h"
#include "util/s3_util.h"

namespace doris::io {

namespace {

std::variant<S3ClientConfig, AzureClientConfig> backend_from_client_conf(const S3ClientConf& conf) {
    if (conf.provider == ObjStorageProvider::AZURE) {
        // Azure has always reused ak/sk as the SharedKey account/key and the bucket as
        // the blob container.
        return AzureClientConfig {
                .endpoint = conf.endpoint,
                .account = conf.ak,
                .container = conf.bucket,
                .auth = AzureSharedKeyConfig {.account_key = conf.sk},
        };
    }
    return S3ClientConfig {
            .vendor = conf.provider,
            .endpoint = conf.endpoint,
            .region = conf.region,
            .need_override_endpoint = conf.need_override_endpoint,
            .use_virtual_addressing = conf.use_virtual_addressing,
            .auth =
                    {
                            .access_key = conf.ak,
                            .secret_key = conf.sk,
                            .session_token = conf.token,
                            .provider_type = conf.cred_provider_type,
                            .role_arn = conf.role_arn,
                            .external_id = conf.external_id,
                    },
    };
}

} // namespace

ObjClientConfig ObjStorageConfigResolver::from_client_conf(const S3ClientConf& conf) {
    return ObjClientConfig {
            .connection =
                    {
                            .max_connections = conf.max_connections,
                            .request_timeout_ms = conf.request_timeout_ms,
                            .connect_timeout_ms = conf.connect_timeout_ms,
                    },
            .usage = conf.is_internal_bucket ? ClientUsage::INTERNAL_STORAGE
                                             : ClientUsage::EXTERNAL_ACCESS,
            .backend = backend_from_client_conf(conf),
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

} // namespace doris::io
