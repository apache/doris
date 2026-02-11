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
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/platform/Environment.h>
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleRequest.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <sstream>

#include "common/logging.h"

namespace doris {

AwsMskIamAuth::AwsMskIamAuth(Config config) : _config(std::move(config)) {
    _credentials_provider = _create_credentials_provider();
}

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> AwsMskIamAuth::_create_credentials_provider() {
    if (!_config.role_arn.empty() && !_config.access_key.empty() && !_config.secret_key.empty()) {
        LOG(INFO) << "Using AWS STS Assume Role with explicit credentials (cross-account): "
                  << _config.role_arn << " (Access Key ID: " << _config.access_key.substr(0, 4)
                  << "****)";

        Aws::Client::ClientConfiguration client_config;
        if (!_config.region.empty()) {
            client_config.region = _config.region;
        }

        // Use explicit AK/SK as base credentials to assume the role
        Aws::Auth::AWSCredentials base_credentials(_config.access_key, _config.secret_key);
        auto base_provider =
                std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(base_credentials);

        auto sts_client = std::make_shared<Aws::STS::STSClient>(base_provider, client_config);

        return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
                _config.role_arn, Aws::String(), /* external_id */ Aws::String(),
                Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, sts_client);
    }
    // 2. Explicit AK/SK credentials (direct access)
    if (!_config.access_key.empty() && !_config.secret_key.empty()) {
        LOG(INFO) << "Using explicit AWS credentials (Access Key ID: "
                  << _config.access_key.substr(0, 4) << "****)";

        Aws::Auth::AWSCredentials credentials(_config.access_key, _config.secret_key);

        return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials);
    }
    // 3. Assume Role with Instance Profile (for same-account access from within AWS)
    if (!_config.role_arn.empty()) {
        LOG(INFO) << "Using AWS STS Assume Role with Instance Profile: " << _config.role_arn;

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
    // 4. AWS Profile (reads from ~/.aws/credentials)
    if (!_config.profile_name.empty()) {
        LOG(INFO) << "Using AWS Profile: " << _config.profile_name;

        return std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
                _config.profile_name.c_str());
    }
    // 5. Custom Credentials Provider
    if (!_config.credentials_provider.empty()) {
        LOG(INFO) << "Using custom credentials provider: " << _config.credentials_provider;

        // Parse credentials provider type string
        std::string provider_upper = _config.credentials_provider;
        std::transform(provider_upper.begin(), provider_upper.end(), provider_upper.begin(),
                      ::toupper);

        if (provider_upper == "ENV" || provider_upper == "ENVIRONMENT") {
            return std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();
        } else if (provider_upper == "INSTANCE_PROFILE" || provider_upper == "INSTANCEPROFILE") {
            return std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
        } else if (provider_upper == "CONTAINER" || provider_upper == "ECS") {
            return std::make_shared<Aws::Auth::TaskRoleCredentialsProvider>(
                    Aws::Environment::GetEnv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").c_str());
        } else if (provider_upper == "DEFAULT") {
            return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
        } else {
            LOG(WARNING) << "Unknown credentials provider type: " << _config.credentials_provider
                        << ", falling back to default credentials provider chain";
            return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
        }
    }
    // No valid credentials configuration found
    LOG(ERROR) << "AWS MSK IAM authentication requires credentials. Please provide.";
    return nullptr;
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
    auto refresh_time =
            _credentials_expiry - std::chrono::milliseconds(_config.token_refresh_margin_ms);
    return now >= refresh_time || _cached_credentials.GetAWSAccessKeyId().empty();
}

Status AwsMskIamAuth::generate_token(const std::string& broker_hostname, std::string* token,
                                     int64_t* token_lifetime_ms) {
    Aws::Auth::AWSCredentials credentials;
    RETURN_IF_ERROR(get_credentials(&credentials));

    std::string timestamp = _get_timestamp();
    std::string date_stamp = _get_date_stamp(timestamp);

    // AWS MSK IAM token is a base64-encoded presigned URL
    // Reference: https://github.com/aws/aws-msk-iam-sasl-signer-python

    // Token expiry in seconds (900 seconds = 15 minutes, matching AWS MSK IAM signer reference)
    static constexpr int TOKEN_EXPIRY_SECONDS = 900;

    // Build the endpoint URL
    std::string endpoint_url = "https://kafka." + _config.region + ".amazonaws.com/";

    // Build credential scope
    std::string credential_scope =
            date_stamp + "/" + _config.region + "/kafka-cluster/aws4_request";

    // Build the canonical query string (sorted alphabetically)
    // IMPORTANT: All query parameters must be included in the signature calculation
    // Session Token must be in canonical query string if using temporary credentials
    std::stringstream canonical_query_ss;
    canonical_query_ss << "Action=kafka-cluster%3AConnect"; // URL-encoded :

    // Add Algorithm
    canonical_query_ss << "&X-Amz-Algorithm=AWS4-HMAC-SHA256";

    // Add Credential
    std::string credential = std::string(credentials.GetAWSAccessKeyId()) + "/" + credential_scope;
    canonical_query_ss << "&X-Amz-Credential=" << _url_encode(credential);

    // Add Date
    canonical_query_ss << "&X-Amz-Date=" << timestamp;

    // Add Expires
    canonical_query_ss << "&X-Amz-Expires=" << TOKEN_EXPIRY_SECONDS;

    // Add Security Token if present (MUST be before signature calculation)
    if (!credentials.GetSessionToken().empty()) {
        canonical_query_ss << "&X-Amz-Security-Token="
                           << _url_encode(std::string(credentials.GetSessionToken()));
    }

    // Add SignedHeaders
    canonical_query_ss << "&X-Amz-SignedHeaders=host";

    std::string canonical_query_string = canonical_query_ss.str();

    // Build the canonical headers
    std::string host = "kafka." + _config.region + ".amazonaws.com";
    std::string canonical_headers = "host:" + host + "\n";
    std::string signed_headers = "host";

    // Build the canonical request
    std::string method = "GET";
    std::string uri = "/";
    std::string payload_hash = _sha256("");

    std::string canonical_request = method + "\n" + uri + "\n" + canonical_query_string + "\n" +
                                    canonical_headers + "\n" + signed_headers + "\n" + payload_hash;

    // Build the string to sign
    std::string algorithm = "AWS4-HMAC-SHA256";
    std::string canonical_request_hash = _sha256(canonical_request);
    std::string string_to_sign =
            algorithm + "\n" + timestamp + "\n" + credential_scope + "\n" + canonical_request_hash;

    // Calculate signature
    std::string signing_key = _calculate_signing_key(std::string(credentials.GetAWSSecretKey()),
                                                     date_stamp, _config.region, "kafka-cluster");
    std::string signature = _hmac_sha256_hex(signing_key, string_to_sign);

    // Build the final presigned URL
    // All parameters are already in canonical_query_string, just add signature
    // Then add User-Agent AFTER signature (not part of signed content, matching reference impl)
    std::string signed_url = endpoint_url + "?" + canonical_query_string +
                             "&X-Amz-Signature=" + signature +
                             "&User-Agent=doris-msk-iam-auth%2F1.0";

    // Base64url encode the signed URL (without padding)
    *token = _base64url_encode(signed_url);

    // Token lifetime in milliseconds
    *token_lifetime_ms = TOKEN_EXPIRY_SECONDS * 1000;

    LOG(INFO) << "Generated AWS MSK IAM token, presigned URL: " << signed_url;

    return Status::OK();
}

std::string AwsMskIamAuth::_hmac_sha256_hex(const std::string& key, const std::string& data) {
    std::string raw = _hmac_sha256(key, data);
    std::stringstream ss;
    for (unsigned char c : raw) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
    }
    return ss.str();
}

std::string AwsMskIamAuth::_url_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (char c : value) {
        // Keep alphanumeric and other accepted characters intact
        if (isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' || c == '.' ||
            c == '~') {
            escaped << c;
        } else {
            // Any other characters are percent-encoded
            escaped << std::uppercase;
            escaped << '%' << std::setw(2) << static_cast<int>(static_cast<unsigned char>(c));
            escaped << std::nouppercase;
        }
    }

    return escaped.str();
}

std::string AwsMskIamAuth::_base64url_encode(const std::string& input) {
    // Standard base64 alphabet
    static const char* base64_chars =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string result;
    result.reserve(((input.size() + 2) / 3) * 4);

    const unsigned char* bytes = reinterpret_cast<const unsigned char*>(input.c_str());
    size_t len = input.size();

    for (size_t i = 0; i < len; i += 3) {
        uint32_t n = static_cast<uint32_t>(bytes[i]) << 16;
        if (i + 1 < len) n |= static_cast<uint32_t>(bytes[i + 1]) << 8;
        if (i + 2 < len) n |= static_cast<uint32_t>(bytes[i + 2]);

        result += base64_chars[(n >> 18) & 0x3F];
        result += base64_chars[(n >> 12) & 0x3F];
        if (i + 1 < len) result += base64_chars[(n >> 6) & 0x3F];
        if (i + 2 < len) result += base64_chars[n & 0x3F];
    }

    // Convert to URL-safe base64 (replace + with -, / with _)
    // and remove padding (=)
    for (char& c : result) {
        if (c == '+')
            c = '-';
        else if (c == '/')
            c = '_';
    }

    return result;
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
    digest = HMAC(EVP_sha256(), key.c_str(), static_cast<int>(key.length()),
                  reinterpret_cast<const unsigned char*>(data.c_str()), data.length(), nullptr,
                  nullptr);
    return {reinterpret_cast<char*>(digest), SHA256_DIGEST_LENGTH};
}

std::string AwsMskIamAuth::_sha256(const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.c_str()), data.length(), hash);

    std::stringstream ss;
    for (unsigned char i : hash) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)i;
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

namespace {
// Property keys for AWS MSK IAM authentication
constexpr const char* PROP_SECURITY_PROTOCOL = "security.protocol";
constexpr const char* PROP_SASL_MECHANISM = "sasl.mechanism";
constexpr const char* PROP_AWS_REGION = "aws.region";
constexpr const char* PROP_AWS_ACCESS_KEY = "aws.access.key";
constexpr const char* PROP_AWS_SECRET_KEY = "aws.secret.key";
constexpr const char* PROP_AWS_ROLE_ARN = "aws.msk.iam.role.arn";
constexpr const char* PROP_AWS_PROFILE_NAME = "aws.profile.name";
constexpr const char* PROP_AWS_CREDENTIALS_PROVIDER = "aws.credentials.provider";
} // namespace

std::unique_ptr<AwsMskIamOAuthCallback> AwsMskIamOAuthCallback::create_from_properties(
        const std::unordered_map<std::string, std::string>& custom_properties,
        const std::string& brokers) {
    auto security_protocol_it = custom_properties.find(PROP_SECURITY_PROTOCOL);
    auto sasl_mechanism_it = custom_properties.find(PROP_SASL_MECHANISM);
    bool is_sasl_ssl = security_protocol_it != custom_properties.end() &&
                       security_protocol_it->second == "SASL_SSL";
    bool is_oauthbearer = sasl_mechanism_it != custom_properties.end() &&
                          sasl_mechanism_it->second == "OAUTHBEARER";

    if (!is_sasl_ssl || !is_oauthbearer) {
        return nullptr;
    }

    // Extract broker hostname for token generation.
    std::string broker_hostname = brokers;
    // If there are multiple brokers, we use the first one (Refrain : is this ok?) 
    if (broker_hostname.find(',') != std::string::npos) {
        broker_hostname = broker_hostname.substr(0, broker_hostname.find(','));
    }
    // Remove port if present
    if (broker_hostname.find(':') != std::string::npos) {
        broker_hostname = broker_hostname.substr(0, broker_hostname.find(':'));
    }

    AwsMskIamAuth::Config auth_config;

    auto region_it = custom_properties.find(PROP_AWS_REGION);
    if (region_it != custom_properties.end()) {
        auth_config.region = region_it->second;
    }

    auto access_key_it = custom_properties.find(PROP_AWS_ACCESS_KEY);
    auto secret_key_it = custom_properties.find(PROP_AWS_SECRET_KEY);
    if (access_key_it != custom_properties.end() && secret_key_it != custom_properties.end()) {
        auth_config.access_key = access_key_it->second;
        auth_config.secret_key = secret_key_it->second;
        LOG(INFO) << "AWS MSK IAM: using explicit credentials (region: " << auth_config.region
                  << ")";
    }

    auto role_arn_it = custom_properties.find(PROP_AWS_ROLE_ARN);
    if (role_arn_it != custom_properties.end()) {
        auth_config.role_arn = role_arn_it->second;
        LOG(INFO) << "AWS MSK IAM: using role " << auth_config.role_arn
                  << " (region: " << auth_config.region << ")";
    }

    auto profile_name_it = custom_properties.find(PROP_AWS_PROFILE_NAME);
    if (profile_name_it != custom_properties.end()) {
        auth_config.profile_name = profile_name_it->second;
        LOG(INFO) << "AWS MSK IAM: using profile " << auth_config.profile_name
                  << " (region: " << auth_config.region << ")";
    }

    auto credentials_provider_it = custom_properties.find(PROP_AWS_CREDENTIALS_PROVIDER);
    if (credentials_provider_it != custom_properties.end()) {
        auth_config.credentials_provider = credentials_provider_it->second;
        LOG(INFO) << "AWS MSK IAM: using credentials provider " << auth_config.credentials_provider
                  << " (region: " << auth_config.region << ")";
    }

    LOG(INFO) << "Enabling AWS MSK IAM authentication for broker: " << broker_hostname
              << ", region: " << auth_config.region;

    auto auth = std::make_shared<AwsMskIamAuth>(auth_config);
    return std::make_unique<AwsMskIamOAuthCallback>(std::move(auth), std::move(broker_hostname));
}

AwsMskIamOAuthCallback::AwsMskIamOAuthCallback(std::shared_ptr<AwsMskIamAuth> auth,
                                               std::string broker_hostname)
        : _auth(std::move(auth)), _broker_hostname(std::move(broker_hostname)) {}

void AwsMskIamOAuthCallback::oauthbearer_token_refresh_cb(
        RdKafka::Handle* handle, const std::string& /*oauthbearer_config*/) {
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

    // Error string for oauthbearer_set_token
    std::string set_token_errstr;

    // Calculate absolute expiry time (Unix timestamp in milliseconds)
    // librdkafka expects an absolute expiry timestamp, not a relative lifetime
    auto now = std::chrono::system_clock::now();
    auto now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    int64_t token_expiry_ms = now_ms + token_lifetime_ms;

    RdKafka::ErrorCode err = handle->oauthbearer_set_token(token, token_expiry_ms, principal,
                                                           extensions, set_token_errstr);

    if (err != RdKafka::ERR_NO_ERROR) {
        LOG(WARNING) << "Failed to set OAuth token: " << RdKafka::err2str(err)
                     << ", detail: " << set_token_errstr;
        handle->oauthbearer_set_token_failure(set_token_errstr.empty() ? RdKafka::err2str(err)
                                                                       : set_token_errstr);
        return;
    }

    LOG(INFO) << "Successfully set AWS MSK IAM OAuth token, lifetime: " << token_lifetime_ms
              << "ms";
}

} // namespace doris
