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

#pragma once

#include <google/cloud/internal/oauth2_credentials.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "cpp/obj-client/auth/gcp/gcp_auth.h"

namespace doris {

enum class GcpTokenScope {
    StorageReadWrite,
    CloudPlatform,
};

// Use one resolved credential for tokens and signing identity. The Google auth
// library handles ADC selection, token caching and refresh internally.
class GcpTokenProvider {
public:
    explicit GcpTokenProvider(const GcpCredentialConfig& credential,
                              const std::string& ca_cert_path = "",
                              GcpTokenScope token_scope = GcpTokenScope::StorageReadWrite);
    explicit GcpTokenProvider(
            std::shared_ptr<google::cloud::oauth2_internal::Credentials> credentials);
    ~GcpTokenProvider();

    GcpTokenProvider(const GcpTokenProvider&) = delete;
    GcpTokenProvider& operator=(const GcpTokenProvider&) = delete;

    std::optional<std::string> get_token() const;
    std::string get_service_account_email() const;

private:
    std::shared_ptr<google::cloud::oauth2_internal::Credentials> _credentials;
    mutable std::mutex _credentials_mutex;
};

} // namespace doris
