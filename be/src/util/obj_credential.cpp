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

#include <gen_cpp/AgentService_types.h>

#include "cpp/obj-storage/auth/gcp/gcp_auth.h"

namespace doris {

GcpCredentialConfig to_gcp_credential_config(const TGcpCredential& credential) {
    GcpCredentialConfig config;
    if (credential.__isset.credential_provider_type) {
        switch (credential.credential_provider_type) {
        case TGcpCredentialProviderType::DEFAULT:
            config.provider_type = GcpCredentialProviderType::Default;
            break;
        case TGcpCredentialProviderType::COMPUTE_ENGINE:
            config.provider_type = GcpCredentialProviderType::ComputeEngine;
            break;
        }
    }
    if (credential.__isset.impersonation_service_account) {
        config.impersonation_service_account = credential.impersonation_service_account;
    }
    return config;
}

} // namespace doris
