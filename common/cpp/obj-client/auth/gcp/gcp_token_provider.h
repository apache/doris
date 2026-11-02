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

#include <google/cloud/credentials.h>
#include <google/cloud/oauth2/access_token_generator.h>

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

// Owns both objects because MakeAccessTokenGenerator takes Credentials by const
// reference. The Google auth library handles token caching and refresh internally.
class GcpTokenProvider {
public:
    explicit GcpTokenProvider(const GcpCredentialConfig& credential,
                              const std::string& ca_cert_path = "",
                              GcpTokenScope token_scope = GcpTokenScope::StorageReadWrite);
    ~GcpTokenProvider();

    GcpTokenProvider(const GcpTokenProvider&) = delete;
    GcpTokenProvider& operator=(const GcpTokenProvider&) = delete;

    std::optional<std::string> get_token() const;

private:
    std::shared_ptr<google::cloud::Credentials> _credentials;
    std::shared_ptr<google::cloud::oauth2::AccessTokenGenerator> _token_generator;
    mutable std::mutex _token_generator_mutex;
};

} // namespace doris
