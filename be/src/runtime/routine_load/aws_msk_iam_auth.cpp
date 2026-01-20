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

#include "runtime/routine_load/aws_msk_iam_auth.h"

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleRequest.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <chrono>
#include <iomanip>
#include <nlohmann/json.hpp>
#include <sstream>

#include "common/logging.h"

namespace doris {

AwsMskIamAuth::AwsMskIamAuth(const Config& config) : _config(config) {
    _credentials_provider = _create_credentials_provider();
}

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> AwsMskIamAuth::_create_credentials_provider() {
    // Priority order:
    // 1. Assume Role (if role_arn is specified)
    // 2. Profile (if profile_name is specified)
    // 3. Instance Profile (if use_instance_profile is true)
    // 4. Default credentials chain (environment variables, ~/.aws/credentials, etc.)

    if (!_config.role_arn.empty()) {
        // Use STS Assume Role
        LOG(INFO) << "Using AWS STS Assume Role: " << _config.role_arn;

        Aws::Client::ClientConfiguration client_config;
        if (!_config.region.empty()) {
            client_config.region = _config.region;
        }

        auto sts_client = std::make_shared<Aws::STS::STSClient>(
                std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>(), client_config);

        return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
                _config.role_arn, Aws::String(), /* external_id */ Aws::String(),
                Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, sts_client);
    }

    if (!_config.profile_name.empty()) {
        LOG(INFO) << "Using AWS profile: " << _config.profile_name;
        return std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
                _config.profile_name.c_str());
    }

    if (_config.use_instance_profile) {
        LOG(INFO) << "Using EC2 Instance Profile credentials";
        return std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
    }

    LOG(INFO) << "Using default AWS credentials provider chain";
    return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
}

Status AwsMskIamAuth::get_credentials(Aws::Auth::AWSCredentials* credentials) {
    std::lock_guard<std::mutex> lock(_mutex);

    // Refresh if needed
    if (_should_refresh_credentials()) {
        _cached_credentials = _credentials_provider->GetAWSCredentials();

        if (_cached_credentials.GetAWSAccessKeyId().empty()) {
            return Status::InternalError("Failed to get AWS credentials");
        }

        // Set expiry time (assume 1 hour for instance profile, or use the credentials expiration)
        _credentials_expiry = std::chrono::system_clock::now() + std::chrono::hours(1);

        LOG(INFO) << "Refreshed AWS credentials for MSK IAM authentication";
    }

    *credentials = _cached_credentials;
    return Status::OK();
}

bool AwsMskIamAuth::_should_refresh_credentials() {
    auto now = std::chrono::system_clock::now();
    auto refresh_time = _credentials_expiry -
                        std::chrono::milliseconds(_config.token_refresh_margin_ms);
    return now >= refresh_time || _cached_credentials.GetAWSAccessKeyId().empty();
}

Status AwsMskIamAuth::generate_token(const std::string& broker_hostname, std::string* token,
                                     int64_t* token_lifetime_ms) {
    Aws::Auth::AWSCredentials credentials;
    RETURN_IF_ERROR(get_credentials(&credentials));

    std::string timestamp = _get_timestamp();
    std::string date_stamp = _get_date_stamp(timestamp);

    // Build the token JSON
    nlohmann::json token_json;
    token_json["version"] = "2020_10_22";
    token_json["host"] = broker_hostname;
    token_json["user-agent"] = "doris-msk-iam-auth/1.0";
    token_json["action"] = "kafka-cluster:Connect";

    // AWS SigV4 fields
    token_json["x-amz-algorithm"] = "AWS4-HMAC-SHA256";

    std::string credential_scope =
            date_stamp + "/" + _config.region + "/kafka-cluster/aws4_request";
    token_json["x-amz-credential"] =
            credentials.GetAWSAccessKeyId().c_str() + std::string("/") + credential_scope;

    token_json["x-amz-date"] = timestamp;

    // Add session token if using temporary credentials
    if (!credentials.GetSessionToken().empty()) {
        token_json["x-amz-security-token"] = credentials.GetSessionToken().c_str();
    }

    token_json["x-amz-signedheaders"] = "host";

    // Generate signature
    std::string signature = _generate_signature_v4(credentials, broker_hostname, timestamp);
    token_json["x-amz-signature"] = signature;

    *token = token_json.dump();

    // Token lifetime is 1 hour (AWS IAM credentials default lifetime)
    // Subtract refresh margin to ensure we refresh before expiry
    *token_lifetime_ms = 3600000 - _config.token_refresh_margin_ms;

    if (VLOG_IS_ON(2)) {
        VLOG(2) << "Generated AWS MSK IAM token for broker: " << broker_hostname;
    }

    return Status::OK();
}

std::string AwsMskIamAuth::_generate_signature_v4(const Aws::Auth::AWSCredentials& credentials,
                                                  const std::string& broker_hostname,
                                                  const std::string& timestamp) {
    std::string date_stamp = _get_date_stamp(timestamp);

    // Step 1: Create canonical request
    std::string method = "GET";
    std::string uri = "/";
    std::string query_string = "Action=kafka-cluster:Connect";
    std::string headers = "host:" + broker_hostname + "\n";
    std::string signed_headers = "host";
    std::string payload_hash = _sha256(""); // Empty payload

    std::string canonical_request = method + "\n" + uri + "\n" + query_string + "\n" + headers +
                                    "\n" + signed_headers + "\n" + payload_hash;

    // Step 2: Create string to sign
    std::string algorithm = "AWS4-HMAC-SHA256";
    std::string credential_scope =
            date_stamp + "/" + _config.region + "/kafka-cluster/aws4_request";
    std::string canonical_request_hash = _sha256(canonical_request);

    std::string string_to_sign = algorithm + "\n" + timestamp + "\n" + credential_scope + "\n" +
                                 canonical_request_hash;

    // Step 3: Calculate signature
    std::string signing_key = _calculate_signing_key(credentials.GetAWSSecretKey().c_str(),
                                                     date_stamp, _config.region, "kafka-cluster");

    std::string signature_bytes = _hmac_sha256(signing_key, string_to_sign);

    // Convert to hex string
    std::stringstream ss;
    for (unsigned char c : signature_bytes) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)c;
    }

    return ss.str();
}

std::string AwsMskIamAuth::_calculate_signing_key(const std::string& secret_key,
                                                  const std::string& date_stamp,
                                                  const std::string& region,
                                                  const std::string& service) {
    std::string k_secret = "AWS4" + secret_key;
    std::string k_date = _hmac_sha256(k_secret, date_stamp);
    std::string k_region = _hmac_sha256(k_date, region);
    std::string k_service = _hmac_sha256(k_region, service);
    std::string k_signing = _hmac_sha256(k_service, "aws4_request");
    return k_signing;
}

std::string AwsMskIamAuth::_hmac_sha256(const std::string& key, const std::string& data) {
    unsigned char* digest;
    digest = HMAC(EVP_sha256(), key.c_str(), key.length(),
                  reinterpret_cast<const unsigned char*>(data.c_str()), data.length(), nullptr,
                  nullptr);
    return std::string(reinterpret_cast<char*>(digest), SHA256_DIGEST_LENGTH);
}

std::string AwsMskIamAuth::_sha256(const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.c_str()), data.length(), hash);

    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return ss.str();
}

std::string AwsMskIamAuth::_get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm_now;
    gmtime_r(&time_t_now, &tm_now);

    std::stringstream ss;
    ss << std::put_time(&tm_now, "%Y%m%dT%H%M%SZ");
    return ss.str();
}

std::string AwsMskIamAuth::_get_date_stamp(const std::string& timestamp) {
    // Extract YYYYMMDD from YYYYMMDDTHHMMSSz
    return timestamp.substr(0, 8);
}

// AwsMskIamOAuthCallback implementation

AwsMskIamOAuthCallback::AwsMskIamOAuthCallback(std::shared_ptr<AwsMskIamAuth> auth,
                                               const std::string& broker_hostname)
        : _auth(auth), _broker_hostname(broker_hostname) {}

void AwsMskIamOAuthCallback::oauthbearer_token_refresh_cb(
        RdKafka::Handle* handle, const std::string& oauthbearer_config) {
    std::string token;
    int64_t token_lifetime_ms;

    Status status = _auth->generate_token(_broker_hostname, &token, &token_lifetime_ms);

    if (!status.ok()) {
        LOG(WARNING) << "Failed to generate AWS MSK IAM token: " << status.to_string();
        std::string errstr = "Failed to generate AWS MSK IAM token: " + status.to_string();
        handle->oauthbearer_set_token_failure(errstr);
        return;
    }

    // Set the token in librdkafka
    // The principal name is not used by AWS MSK but required by the API
    std::string principal = "doris-consumer";

    // Extensions (optional additional fields)
    std::list<std::string> extensions;

    RdKafka::ErrorCode err =
            handle->oauthbearer_set_token(token, token_lifetime_ms, principal, extensions);

    if (err != RdKafka::ERR_NO_ERROR) {
        LOG(WARNING) << "Failed to set OAuth token: " << RdKafka::err2str(err);
        handle->oauthbearer_set_token_failure(RdKafka::err2str(err));
        return;
    }

    LOG(INFO) << "Successfully set AWS MSK IAM OAuth token, lifetime: " << token_lifetime_ms
              << "ms";
}

} // namespace doris
