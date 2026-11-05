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

#include "io/obj_storage/azure_config_codec.h"

#include <gen_cpp/PlanNodes_types.h>

#include <string>

#include "common/check.h"

namespace doris::io {

Result<AzureClientConfig> AzureConfigCodec::decode(const TAzureClientConfig& config,
                                                   std::string_view location_container) {
    AzureClientConfig azure {
            .endpoint = config.endpoint,
            .account = config.account_name,
            .container = std::string(location_container),
            .auth = AzureSharedKeyConfig {},
    };

    if (config.__isset.container && !config.container.empty()) {
        if (!location_container.empty() && location_container != config.container) {
            return ResultError(Status::InvalidArgument<false>(
                    "Azure container '{}' of the file location does not match the configured "
                    "container '{}'",
                    std::string(location_container), config.container));
        }
        azure.container = config.container;
    }

    // A Thrift union has no compile-time guarantee that exactly one member is set: an unset
    // credential or an old/new peer setting two members both arrive here as valid messages.
    const TAzureCredential& credential = config.credential;
    int set_count = static_cast<int>(credential.__isset.shared_key) +
                    static_cast<int>(credential.__isset.sas) +
                    static_cast<int>(credential.__isset.client_secret);
    if (!config.__isset.credential || set_count != 1) {
        return ResultError(Status::InvalidArgument<false>(
                "Azure client config must carry exactly one credential, got {}",
                config.__isset.credential ? set_count : 0));
    }

    if (credential.__isset.shared_key) {
        azure.auth = AzureSharedKeyConfig {
                .account_key = credential.shared_key.account_key,
        };
    } else if (credential.__isset.sas) {
        std::string_view token = credential.sas.token;
        if (!token.empty() && token.front() == '?') {
            token.remove_prefix(1);
        }
        azure.auth = AzureSasConfig {
                .token = std::string(token),
                .expires_at_ms = credential.sas.__isset.expires_at_ms
                                         ? std::optional<int64_t>(credential.sas.expires_at_ms)
                                         : std::nullopt,
        };
    } else {
        DORIS_CHECK(credential.__isset.client_secret);
        const TAzureClientSecret& secret = credential.client_secret;
        azure.auth = AzureClientSecretConfig {
                .tenant_id = secret.tenant_id,
                .client_id = secret.client_id,
                .client_secret = secret.client_secret,
                .authority_host = secret.authority_host,
        };
    }
    return azure;
}

} // namespace doris::io
