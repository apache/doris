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

// Type alias for OSS Credentials
using OSSCredentials = AlibabaCloud::OSS::Credentials;

/**
 * @brief ECS Metadata Service Credential Provider for Alibaba Cloud OSS
 *
 * Fetches temporary credentials (AccessKeyId, AccessKeySecret, SecurityToken)
 * from ECS instance metadata service at http://100.100.100.200/latest/meta-data/
 *
 * Features:
 * - Automatic credential caching
 * - Auto-refresh 5 minutes before expiration
 * - Thread-safe access
 * - Implements AlibabaCloud::OSS::CredentialsProvider interface
 *
 * Usage:
 *   auto provider = std::make_shared<ECSMetadataCredentialsProvider>();
 *   OSSClient client(endpoint, provider, config);
 */
class ECSMetadataCredentialsProvider : public AlibabaCloud::OSS::CredentialsProvider {
public:
    ECSMetadataCredentialsProvider();
    ~ECSMetadataCredentialsProvider() override = default;

    /**
     * @brief Get OSS credentials (from cache or fetch new from ECS metadata)
     *
     * This method is called by OssClient when making API calls.
     * Thread-safe: Can be called from multiple threads.
     *
     * @return Credentials object with AccessKeyId, AccessKeySecret, SecurityToken
     * @throws std::runtime_error if metadata service is unreachable or returns error
     */
    AlibabaCloud::OSS::Credentials getCredentials() override;

private:
    /**
     * @brief Fetch credentials from ECS metadata service
     *
     * Makes HTTP GET requests to:
     * 1. http://100.100.100.200/latest/meta-data/ram/security-credentials/ (get role name)
     * 2. http://100.100.100.200/latest/meta-data/ram/security-credentials/{role-name} (get creds)
     *
     * @return 0 on success, non-zero on error
     */
    int _fetch_credentials_from_metadata();

    /**
     * @brief Get RAM role name from metadata service
     *
     * @param role_name [out] RAM role name (e.g., "DorisOSSRole")
     * @return 0 on success, non-zero on error
     */
    int _get_role_name(std::string& role_name);

    /**
     * @brief Get credentials for a specific RAM role
     *
     * @param role_name RAM role name
     * @return 0 on success, updates _cached_credentials
     */
    int _get_credentials_from_role(const std::string& role_name);

    /**
     * @brief Check if cached credentials are expired or about to expire
     *
     * @return true if expired or will expire within REFRESH_BEFORE_EXPIRY_SECONDS
     */
    bool _is_expired() const;

    /**
     * @brief Helper method to make HTTP GET request
     *
     * Uses libcurl to make HTTP request to metadata service
     *
     * @param url Full URL to fetch
     * @param response [out] Response body
     * @return 0 on HTTP 200, non-zero otherwise
     */
    int _http_get(const std::string& url, std::string& response);

    // Mutex to protect _cached_credentials and _expiration
    mutable std::mutex _mtx;

    // Cached credentials
    std::unique_ptr<OSSCredentials> _cached_credentials;

    // Expiration time of cached credentials
    std::chrono::system_clock::time_point _expiration;

    // ECS metadata service configuration
    static constexpr const char* METADATA_SERVICE_HOST = "100.100.100.200";
    static constexpr const char* METADATA_SERVICE_PATH = "/latest/meta-data/ram/security-credentials/";
    static constexpr int METADATA_SERVICE_TIMEOUT_MS = 5000; // 5 seconds
    static constexpr int REFRESH_BEFORE_EXPIRY_SECONDS = 300; // 5 minutes
};

} // namespace doris
