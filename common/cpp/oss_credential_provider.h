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

#include <chrono>
#include <memory>
#include <mutex>
#include <string>

// Include OSS SDK headers for credential provider interface
#include <alibabacloud/oss/auth/Credentials.h>
#include <alibabacloud/oss/auth/CredentialsProvider.h>

namespace doris {

using OSSCredentials = AlibabaCloud::OSS::Credentials;

// Fetches temporary credentials from ECS metadata service (thread-safe, auto-refresh)
class ECSMetadataCredentialsProvider : public AlibabaCloud::OSS::CredentialsProvider {
public:
    ECSMetadataCredentialsProvider();
    ~ECSMetadataCredentialsProvider() override = default;

    AlibabaCloud::OSS::Credentials getCredentials() override;

private:
    int _fetch_credentials_from_metadata();
    int _fetch_credentials_outside_lock(
            std::unique_ptr<AlibabaCloud::OSS::Credentials>& out_credentials,
            std::chrono::system_clock::time_point& out_expiration);
    int _get_role_name(std::string& role_name);
    int _get_credentials_from_role(const std::string& role_name);
    bool _is_expired() const;
    int _http_get(const std::string& url, std::string& response);

    mutable std::mutex _mtx;
    std::unique_ptr<OSSCredentials> _cached_credentials;
    std::chrono::system_clock::time_point _expiration;

    static constexpr const char* METADATA_SERVICE_HOST = "100.100.100.200";
    static constexpr const char* METADATA_SERVICE_PATH = "/latest/meta-data/ram/security-credentials/";
    static constexpr int METADATA_SERVICE_TIMEOUT_MS = 5000;
    static constexpr int REFRESH_BEFORE_EXPIRY_SECONDS = 300;
};

} // namespace doris
