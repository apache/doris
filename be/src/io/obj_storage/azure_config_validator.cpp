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

#include "io/obj_storage/azure_config_validator.h"

#include <strings.h>

#include <string>
#include <string_view>
#include <variant>

#include "io/obj_storage/obj_storage_uri.h"

namespace doris::io {
namespace {

Status require_azure_field(bool present, const char* field) {
    if (!present) {
        return Status::InvalidArgument<false>("Azure client config is missing {}", field);
    }
    return Status::OK();
}

Status validate_azure_auth_fields(const AzureClientConfig& config,
                                  const AzureSharedKeyConfig& shared_key) {
    RETURN_IF_ERROR(require_azure_field(!config.account.empty(), "account_name for SharedKey"));
    return require_azure_field(!shared_key.account_key.empty(), "account_key for SharedKey");
}

Status validate_azure_auth_fields(const AzureClientConfig& /*config*/, const AzureSasConfig& sas) {
    return require_azure_field(!sas.token.empty(), "token for SAS");
}

Status validate_azure_auth_fields(const AzureClientConfig& /*config*/,
                                  const AzureClientSecretConfig& client_secret) {
    RETURN_IF_ERROR(
            require_azure_field(!client_secret.tenant_id.empty(), "tenant_id for client secret"));
    RETURN_IF_ERROR(
            require_azure_field(!client_secret.client_id.empty(), "client_id for client secret"));
    return require_azure_field(!client_secret.client_secret.empty(),
                               "client_secret for client secret");
}

} // namespace

Status AzureConfigValidator::validate(const AzureClientConfig& config) {
    RETURN_IF_ERROR(require_azure_field(!config.endpoint.empty(), "endpoint"));
    RETURN_IF_ERROR(require_azure_field(!config.container.empty(), "container"));
    RETURN_IF_ERROR(
            std::visit([&](const auto& auth) { return validate_azure_auth_fields(config, auth); },
                       config.auth));

    std::string_view host_account = ObjStorageUri::azure_account_of_endpoint(config.endpoint);
    if (!config.account.empty() && !host_account.empty() &&
        (host_account.size() != config.account.size() ||
         strncasecmp(host_account.data(), config.account.data(), host_account.size()) != 0)) {
        return Status::InvalidArgument<false>("Azure account '{}' does not match endpoint '{}'",
                                              config.account, config.endpoint);
    }
    return Status::OK();
}

} // namespace doris::io
