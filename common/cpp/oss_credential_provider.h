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

#ifdef USE_OSS

#include <alibabacloud/oss/auth/Credentials.h>
#include <alibabacloud/oss/auth/CredentialsProvider.h>

namespace doris {

// Fetches temporary credentials from the ECS instance metadata service (100.100.100.200)
// and auto-refreshes before expiry.
class ECSMetadataCredentialsProvider : public AlibabaCloud::OSS::CredentialsProvider {
public:
    ECSMetadataCredentialsProvider();
    ~ECSMetadataCredentialsProvider() override = default;

    AlibabaCloud::OSS::Credentials getCredentials() override;

private:
    int _fetch_credentials_outside_lock(
            std::unique_ptr<AlibabaCloud::OSS::Credentials>& out_credentials,
            std::chrono::system_clock::time_point& out_expiration);
    int _get_role_name(std::string& role_name);
    bool _is_expired() const;
    int _http_get(const std::string& url, std::string& response);

    mutable std::mutex _mtx;
    std::unique_ptr<AlibabaCloud::OSS::Credentials> _cached_credentials;
    std::chrono::system_clock::time_point _expiration;

    static constexpr const char* METADATA_SERVICE_HOST = "100.100.100.200";
    static constexpr const char* METADATA_SERVICE_PATH =
            "/latest/meta-data/ram/security-credentials/";
    static constexpr int METADATA_SERVICE_TIMEOUT_MS = 5000;
    static constexpr int REFRESH_BEFORE_EXPIRY_SECONDS = 300;
};

// Obtains temporary credentials via Alibaba STS AssumeRole and auto-refreshes before expiry.
// Uses the alibabacloud-sdk-cpp stack (sts-20150401 + credentials-cpp).
class OSSSTSCredentialProvider : public AlibabaCloud::OSS::CredentialsProvider {
public:
    explicit OSSSTSCredentialProvider(const std::string& role_arn, const std::string& region,
                                      const std::string& external_id = "",
                                      const std::string& ca_cert_path = "",
                                      const std::string& sts_endpoint = "");
    ~OSSSTSCredentialProvider() override = default;

    AlibabaCloud::OSS::Credentials getCredentials() override;

private:
    int _fetch_credentials_from_sts(
            std::unique_ptr<AlibabaCloud::OSS::Credentials>& out_credentials,
            std::chrono::system_clock::time_point& out_expiration);
    bool _is_expired() const;

    mutable std::mutex _mtx;
    std::unique_ptr<AlibabaCloud::OSS::Credentials> _cached_credentials;
    std::chrono::system_clock::time_point _expiration;
    std::string _role_arn;
    std::string _region;
    std::string _external_id;
    std::string _ca_cert_path;
    std::string _sts_endpoint;

    static constexpr int REFRESH_BEFORE_EXPIRY_SECONDS = 300;
    static constexpr int SESSION_DURATION_SECONDS = 3600;
};

// Default Alibaba Cloud credential provider chain (env vars, config file, ECS metadata).
class OSSDefaultCredentialsProvider : public AlibabaCloud::OSS::CredentialsProvider {
public:
    OSSDefaultCredentialsProvider();
    ~OSSDefaultCredentialsProvider() override = default;

    AlibabaCloud::OSS::Credentials getCredentials() override;

private:
    bool _is_expired() const;

    mutable std::mutex _mtx;
    std::unique_ptr<AlibabaCloud::OSS::Credentials> _cached_credentials;
    std::chrono::system_clock::time_point _expiration;

    static constexpr int REFRESH_BEFORE_EXPIRY_SECONDS = 300;
};

} // namespace doris

#endif // USE_OSS
