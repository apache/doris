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

#include "cpp/oss_credential_provider.h"

#ifdef USE_OSS

#include <curl/curl.h>
#include <rapidjson/document.h>
#include <time.h>

#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "common/logging.h"

#include <alibabacloud/oss/auth/Credentials.h>
#include <alibabacloud/oss/auth/CredentialsProvider.h>
#include <alibabacloud/credentials/Client.hpp>
#include <alibabacloud/Sts20150401.hpp>
#include <alibabacloud/utils/models/Config.hpp>
#include <darabonba/Runtime.hpp>

namespace {
std::string mask_credential(const std::string& cred) {
    if (cred.empty()) return "";
    size_t len = cred.length();
    if (len <= 8) {
        if (len <= 4) return std::string(len, '*');
        return cred.substr(0, 2) + std::string(len - 4, '*') + cred.substr(len - 2);
    }
    return cred.substr(0, 4) + std::string(len - 8, '*') + cred.substr(len - 4);
}
} // namespace

namespace doris {

static size_t curl_write_callback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    size_t total_size = size * nmemb;
    userp->append(static_cast<char*>(contents), total_size);
    return total_size;
}

// ---- ECSMetadataCredentialsProvider ----

ECSMetadataCredentialsProvider::ECSMetadataCredentialsProvider()
        : _cached_credentials(nullptr), _expiration(std::chrono::system_clock::now()) {
    LOG(INFO) << "ECSMetadataCredentialsProvider initialized";
}

bool ECSMetadataCredentialsProvider::_is_expired() const {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(_expiration - now).count() <=
           REFRESH_BEFORE_EXPIRY_SECONDS;
}

AlibabaCloud::OSS::Credentials ECSMetadataCredentialsProvider::getCredentials() {
    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials && !_is_expired()) {
            VLOG(2) << "Returning cached ECS metadata credentials";
            return *_cached_credentials;
        }
    }

    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials) {
            auto t = std::chrono::system_clock::to_time_t(_expiration);
            LOG(INFO) << "ECS metadata credentials expiring ("
                      << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S")
                      << "), refreshing";
        } else {
            LOG(INFO) << "Fetching ECS metadata credentials (first time)";
        }
    }

    std::unique_ptr<AlibabaCloud::OSS::Credentials> new_credentials;
    std::chrono::system_clock::time_point new_expiration;

    if (_fetch_credentials_outside_lock(new_credentials, new_expiration) != 0) {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials) {
            LOG(WARNING) << "Using expired ECS metadata credentials as fallback";
            return *_cached_credentials;
        }
        throw std::runtime_error("Failed to fetch credentials from ECS metadata service");
    }

    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials && !_is_expired()) {
            return *_cached_credentials;
        }
        _cached_credentials = std::move(new_credentials);
        _expiration = new_expiration;
        auto t = std::chrono::system_clock::to_time_t(_expiration);
        LOG(INFO) << "ECS metadata credentials refreshed, expiry: "
                  << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S");
        return *_cached_credentials;
    }
}

int ECSMetadataCredentialsProvider::_http_get(const std::string& url, std::string& response) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        LOG(ERROR) << "Failed to initialize CURL";
        return -1;
    }

    response.clear();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, METADATA_SERVICE_TIMEOUT_MS);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        LOG(ERROR) << "ECS metadata HTTP GET failed: " << curl_easy_strerror(res);
        curl_easy_cleanup(curl);
        return -1;
    }

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    if (http_code != 200) {
        LOG(ERROR) << "ECS metadata service returned HTTP " << http_code;
        return -1;
    }
    return 0;
}

int ECSMetadataCredentialsProvider::_get_role_name(std::string& role_name) {
    std::string url = std::string("http://") + METADATA_SERVICE_HOST + METADATA_SERVICE_PATH;
    std::string response;
    if (_http_get(url, response) != 0) {
        return -1;
    }

    role_name = response;
    role_name.erase(role_name.begin(),
                    std::find_if(role_name.begin(), role_name.end(),
                                 [](unsigned char ch) { return !std::isspace(ch); }));
    role_name.erase(std::find_if(role_name.rbegin(), role_name.rend(),
                                 [](unsigned char ch) { return !std::isspace(ch); })
                            .base(),
                    role_name.end());

    if (role_name.empty()) {
        LOG(ERROR) << "No RAM role attached to this ECS instance";
        return -1;
    }

    size_t newline_pos = role_name.find('\n');
    if (newline_pos != std::string::npos) {
        std::string all_roles = role_name;
        role_name = role_name.substr(0, newline_pos);
        LOG(WARNING) << "Multiple RAM roles found, using first: " << role_name
                     << " (all: " << all_roles << ")";
    }

    LOG(INFO) << "ECS RAM role: " << role_name;
    return 0;
}

int ECSMetadataCredentialsProvider::_fetch_credentials_outside_lock(
        std::unique_ptr<AlibabaCloud::OSS::Credentials>& out_credentials,
        std::chrono::system_clock::time_point& out_expiration) {
    std::string role_name;
    if (_get_role_name(role_name) != 0) {
        return -1;
    }

    std::string url =
            std::string("http://") + METADATA_SERVICE_HOST + METADATA_SERVICE_PATH + role_name;
    std::string response;
    if (_http_get(url, response) != 0) {
        return -1;
    }

    rapidjson::Document doc;
    doc.Parse(response.c_str());

    if (doc.HasParseError()) {
        LOG(ERROR) << "Failed to parse ECS metadata JSON response";
        return -1;
    }
    if (!doc.HasMember("Code") || std::string(doc["Code"].GetString()) != "Success") {
        LOG(ERROR) << "ECS metadata error: "
                   << (doc.HasMember("Message") ? doc["Message"].GetString() : "unknown");
        return -1;
    }
    if (!doc.HasMember("AccessKeyId") || !doc.HasMember("AccessKeySecret") ||
        !doc.HasMember("SecurityToken") || !doc.HasMember("Expiration")) {
        LOG(ERROR) << "ECS metadata response missing required fields";
        return -1;
    }

    std::string ak = doc["AccessKeyId"].GetString();
    std::string sk = doc["AccessKeySecret"].GetString();
    std::string token = doc["SecurityToken"].GetString();
    std::string expiry_str = doc["Expiration"].GetString();

    if (ak.empty() || sk.empty() || token.empty()) {
        LOG(ERROR) << "ECS metadata returned empty credentials";
        return -1;
    }

    std::tm tm = {};
    std::istringstream ss(expiry_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    if (ss.fail()) {
        LOG(ERROR) << "Failed to parse expiration from ECS metadata: " << expiry_str;
        return -1;
    }

    out_expiration = std::chrono::system_clock::from_time_t(timegm(&tm));
    out_credentials = std::make_unique<AlibabaCloud::OSS::Credentials>(ak, sk, token);
    VLOG(1) << "ECS metadata credentials: ak=" << mask_credential(ak) << ", expiry=" << expiry_str;
    return 0;
}

// ---- OSSSTSCredentialProvider ----

OSSSTSCredentialProvider::OSSSTSCredentialProvider(const std::string& role_arn,
                                                     const std::string& region,
                                                     const std::string& external_id)
        : _cached_credentials(nullptr),
          _expiration(std::chrono::system_clock::now()),
          _role_arn(role_arn),
          _region(region),
          _external_id(external_id) {
    if (_role_arn.empty()) {
        throw std::invalid_argument("RAM role ARN cannot be empty for STS AssumeRole");
    }
    LOG(INFO) << "OSSSTSCredentialProvider: role_arn=" << _role_arn << ", region=" << _region
              << ", external_id=" << (_external_id.empty() ? "(none)" : mask_credential(_external_id));
}

bool OSSSTSCredentialProvider::_is_expired() const {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(_expiration - now).count() <=
           REFRESH_BEFORE_EXPIRY_SECONDS;
}

AlibabaCloud::OSS::Credentials OSSSTSCredentialProvider::getCredentials() {
    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials && !_is_expired()) {
            VLOG(2) << "Returning cached STS AssumeRole credentials";
            return *_cached_credentials;
        }
    }

    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials) {
            auto t = std::chrono::system_clock::to_time_t(_expiration);
            LOG(INFO) << "STS credentials expiring ("
                      << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S")
                      << "), refreshing";
        } else {
            LOG(INFO) << "Fetching STS AssumeRole credentials (first time)";
        }
    }

    std::unique_ptr<AlibabaCloud::OSS::Credentials> new_credentials;
    std::chrono::system_clock::time_point new_expiration;

    if (_fetch_credentials_from_sts(new_credentials, new_expiration) != 0) {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials) {
            LOG(WARNING) << "Using expired STS credentials as fallback";
            return *_cached_credentials;
        }
        throw std::runtime_error("Failed to fetch credentials via STS AssumeRole");
    }

    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials && !_is_expired()) {
            return *_cached_credentials;
        }
        _cached_credentials = std::move(new_credentials);
        _expiration = new_expiration;
        auto t = std::chrono::system_clock::to_time_t(_expiration);
        LOG(INFO) << "STS AssumeRole credentials refreshed, expiry: "
                  << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S");
        return *_cached_credentials;
    }
}

int OSSSTSCredentialProvider::_fetch_credentials_from_sts(
        std::unique_ptr<AlibabaCloud::OSS::Credentials>& out_credentials,
        std::chrono::system_clock::time_point& out_expiration) {
    try {
        AlibabaCloud::Credentials::Client cred_client;
        AlibabaCloud::Credentials::Models::CredentialModel base_cred = cred_client.getCredential();
        LOG(INFO) << "STS AssumeRole base credentials from provider: " << base_cred.getProviderName();

        AlibabaCloud::OpenApi::Utils::Models::Config config;
        config.setAccessKeyId(base_cred.getAccessKeyId());
        config.setAccessKeySecret(base_cred.getAccessKeySecret());
        if (!base_cred.getSecurityToken().empty()) {
            config.setSecurityToken(base_cred.getSecurityToken());
        }
        config.setRegionId(_region);
        config.setEndpoint("sts." + _region + ".aliyuncs.com");

        AlibabaCloud::Sts20150401::Client client(config);

        AlibabaCloud::Sts20150401::Models::AssumeRoleRequest request;
        request.setRoleArn(_role_arn);
        request.setRoleSessionName("doris-oss-session");
        request.setDurationSeconds(SESSION_DURATION_SECONDS);
        if (!_external_id.empty()) {
            request.setExternalId(_external_id);
        }

        Darabonba::RuntimeOptions runtime;
        runtime.setIgnoreSSL(true);

        AlibabaCloud::Sts20150401::Models::AssumeRoleResponse response =
                client.assumeRoleWithOptions(request, runtime);

        auto body = response.getBody();
        auto creds = body.getCredentials();

        std::string ak = creds.getAccessKeyId();
        std::string sk = creds.getAccessKeySecret();
        std::string token = creds.getSecurityToken();
        std::string expiry_str = creds.getExpiration();

        if (ak.empty() || sk.empty() || token.empty()) {
            LOG(ERROR) << "STS AssumeRole returned empty credentials";
            return -1;
        }

        std::tm tm = {};
        std::istringstream ss(expiry_str);
        ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
        if (ss.fail()) {
            LOG(ERROR) << "Failed to parse STS expiration: " << expiry_str;
            return -1;
        }

        out_expiration = std::chrono::system_clock::from_time_t(timegm(&tm));
        out_credentials = std::make_unique<AlibabaCloud::OSS::Credentials>(ak, sk, token);
        VLOG(1) << "STS credentials: ak=" << mask_credential(ak) << ", expiry=" << expiry_str;
        return 0;

    } catch (const std::exception& e) {
        LOG(ERROR) << "STS AssumeRole failed: " << e.what();
        return -1;
    }
}

// ---- OSSDefaultCredentialsProvider ----

OSSDefaultCredentialsProvider::OSSDefaultCredentialsProvider()
        : _cached_credentials(nullptr), _expiration(std::chrono::system_clock::now()) {
    LOG(INFO) << "OSSDefaultCredentialsProvider initialized";
}

bool OSSDefaultCredentialsProvider::_is_expired() const {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(_expiration - now).count() <=
           REFRESH_BEFORE_EXPIRY_SECONDS;
}

AlibabaCloud::OSS::Credentials OSSDefaultCredentialsProvider::getCredentials() {
    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials && !_is_expired()) {
            VLOG(2) << "Returning cached default provider credentials";
            return *_cached_credentials;
        }
    }

    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials) {
            LOG(INFO) << "Default provider credentials expiring, refreshing";
        } else {
            LOG(INFO) << "Fetching OSS credentials via default provider chain";
        }
    }

    try {
        AlibabaCloud::Credentials::Client cred_client;
        AlibabaCloud::Credentials::Models::CredentialModel cred_model = cred_client.getCredential();

        std::string ak = cred_model.getAccessKeyId();
        std::string sk = cred_model.getAccessKeySecret();
        std::string token = cred_model.getSecurityToken();

        if (ak.empty() || sk.empty()) {
            std::lock_guard<std::mutex> lock(_mtx);
            if (_cached_credentials) {
                LOG(WARNING) << "Default provider returned empty credentials, using fallback";
                return *_cached_credentials;
            }
            throw std::runtime_error("Default provider chain returned empty credentials");
        }

        auto new_credentials = token.empty()
                ? std::make_unique<AlibabaCloud::OSS::Credentials>(ak, sk)
                : std::make_unique<AlibabaCloud::OSS::Credentials>(ak, sk, token);

        auto new_expiration = std::chrono::system_clock::now() + std::chrono::hours(1);

        {
            std::lock_guard<std::mutex> lock(_mtx);
            _cached_credentials = std::move(new_credentials);
            _expiration = new_expiration;
            auto t = std::chrono::system_clock::to_time_t(_expiration);
            LOG(INFO) << "Default provider credentials fetched, provider: "
                      << cred_model.getProviderName() << ", expiry: "
                      << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S");
            return *_cached_credentials;
        }
    } catch (const std::exception& e) {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials) {
            LOG(WARNING) << "Default provider failed, using expired credentials as fallback: "
                         << e.what();
            return *_cached_credentials;
        }
        throw std::runtime_error(std::string("Failed to get OSS credentials: ") + e.what());
    }
}

} // namespace doris

#endif // USE_OSS
