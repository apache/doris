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

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <mutex>
#include <string>

#include "common/status.h"

namespace doris {

/**
 * AWS MSK IAM authentication token generator.
 * 
 * This class generates SASL/OAUTHBEARER tokens for AWS MSK IAM authentication.
 * It uses AWS SDK for C++ to obtain IAM credentials and generates signed tokens
 * that can be used with librdkafka's OAUTHBEARER mechanism.
 * 
 * The token format follows AWS MSK IAM authentication protocol:
 * - Token is a JSON object containing AWS signature v4 authentication
 * - Includes AWS access key, secret key, session token (if using temporary credentials)
 * - Token lifetime is tied to the IAM credentials lifetime (typically 1-12 hours)
 * 
 * Thread-safe: Yes (uses mutex for credential refresh)
 */
class AwsMskIamAuth {
public:
    /**
     * Configuration for AWS MSK IAM authentication
     */
    struct Config {
        std::string region;                    // AWS region (e.g., "us-east-1")
        std::string role_arn;                  // IAM role ARN (optional, for assume role)
        std::string profile_name;              // AWS profile name (optional)
        bool use_instance_profile = true;     // Use EC2 instance profile
        int token_refresh_margin_ms = 60000;   // Refresh token 60s before expiry
    };

    explicit AwsMskIamAuth(const Config& config);
    ~AwsMskIamAuth() = default;

    /**
     * Generate AWS MSK IAM authentication token.
     * 
     * The token is a JSON string containing:
     * {
     *   "version": "2020_10_22",
     *   "host": "<broker-hostname>",
     *   "user-agent": "doris-msk-iam-auth",
     *   "action": "kafka-cluster:Connect",
     *   "x-amz-algorithm": "AWS4-HMAC-SHA256",
     *   "x-amz-credential": "<access-key>/<date>/<region>/kafka-cluster/aws4_request",
     *   "x-amz-date": "<timestamp>",
     *   "x-amz-security-token": "<session-token>",  // if using temporary credentials
     *   "x-amz-signature": "<signature>",
     *   "x-amz-signedheaders": "host"
     * }
     * 
     * @param broker_hostname The MSK broker hostname
     * @param token_lifetime_ms Output: token lifetime in milliseconds
     * @return Status and the generated token string
     */
    Status generate_token(const std::string& broker_hostname, std::string* token,
                          int64_t* token_lifetime_ms);

    /**
     * Get current AWS credentials.
     * This will refresh credentials if they are expired or about to expire.
     */
    Status get_credentials(Aws::Auth::AWSCredentials* credentials);

private:
    /**
     * Create AWS credentials provider based on configuration
     */
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> _create_credentials_provider();

    /**
     * Generate AWS Signature Version 4
     */
    std::string _generate_signature_v4(const Aws::Auth::AWSCredentials& credentials,
                                       const std::string& broker_hostname,
                                       const std::string& timestamp);

    /**
     * Calculate AWS SigV4 signing key
     */
    std::string _calculate_signing_key(const std::string& secret_key,
                                       const std::string& date_stamp, const std::string& region,
                                       const std::string& service);

    /**
     * HMAC-SHA256 helper
     */
    std::string _hmac_sha256(const std::string& key, const std::string& data);

    /**
     * SHA256 hash helper
     */
    std::string _sha256(const std::string& data);

    /**
     * Get current timestamp in ISO8601 format
     */
    std::string _get_timestamp();

    /**
     * Get date stamp from timestamp
     */
    std::string _get_date_stamp(const std::string& timestamp);

    /**
     * Check if credentials need refresh
     */
    bool _should_refresh_credentials();

    Config _config;
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> _credentials_provider;
    std::mutex _mutex;
    Aws::Auth::AWSCredentials _cached_credentials;
    std::chrono::time_point<std::chrono::system_clock> _credentials_expiry;
};

/**
 * librdkafka OAUTHBEARER callback for AWS MSK IAM authentication.
 * 
 * This callback is invoked by librdkafka when it needs to refresh the
 * OAUTHBEARER token. It uses AwsMskIamAuth to generate the token.
 */
class AwsMskIamOAuthCallback : public RdKafka::OAuthBearerTokenRefreshCb {
public:
    explicit AwsMskIamOAuthCallback(std::shared_ptr<AwsMskIamAuth> auth,
                                    const std::string& broker_hostname);

    /**
     * Callback invoked by librdkafka to refresh OAuth token.
     * 
     * @param handle The Kafka handle (consumer or producer)
     * @param oauthbearer_config Configuration string from 'sasl.oauthbearer.config'
     */
    void oauthbearer_token_refresh_cb(RdKafka::Handle* handle,
                                      const std::string& oauthbearer_config) override;

private:
    std::shared_ptr<AwsMskIamAuth> _auth;
    std::string _broker_hostname;
};

} // namespace doris
