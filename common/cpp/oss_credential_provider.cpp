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

#include <alibabacloud/oss/auth/Credentials.h>
#include <alibabacloud/oss/auth/CredentialsProvider.h>
#include <curl/curl.h>
#include <rapidjson/document.h>
#include <unistd.h>

#include <alibabacloud/Sts20150401.hpp>
#include <alibabacloud/credentials/Client.hpp>
#include <alibabacloud/utils/models/Config.hpp>
#include <ctime>
#include <darabonba/Runtime.hpp>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "common/logging.h"

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

size_t oss_curl_write_cb(void* contents, size_t size, size_t nmemb, std::string* userp) {
    userp->append(static_cast<char*>(contents), size * nmemb);
    return size * nmemb;
}
} // namespace

namespace doris {

// ── ECSMetadataCredentialsProvider ───────────────────────────────────────────

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
            return *_cached_credentials;
        }
        if (_cached_credentials) {
            auto t = std::chrono::system_clock::to_time_t(_expiration);
            struct tm tm_buf;
            LOG(INFO) << "ECS credentials expiring ("
                      << std::put_time(localtime_r(&t, &tm_buf), "%Y-%m-%d %H:%M:%S")
                      << "), refreshing";
        } else {
            LOG(INFO) << "Fetching ECS metadata credentials (first time)";
        }
    }

    std::unique_ptr<AlibabaCloud::OSS::Credentials> new_creds;
    std::chrono::system_clock::time_point new_expiry;

    if (_fetch_credentials_outside_lock(new_creds, new_expiry) != 0) {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials) {
            LOG(WARNING) << "Using expired ECS credentials as fallback";
            return *_cached_credentials;
        }
        LOG(ERROR) << "Failed to fetch ECS credentials with no cached fallback";
        return AlibabaCloud::OSS::Credentials("", "", "");
    }

    std::lock_guard<std::mutex> lock(_mtx);
    if (_cached_credentials && !_is_expired()) {
        return *_cached_credentials;
    }
    _cached_credentials = std::move(new_creds);
    _expiration = new_expiry;
    auto t = std::chrono::system_clock::to_time_t(_expiration);
    struct tm tm_buf;
    LOG(INFO) << "ECS credentials refreshed, expiry: "
              << std::put_time(localtime_r(&t, &tm_buf), "%Y-%m-%d %H:%M:%S");
    return *_cached_credentials;
}

int ECSMetadataCredentialsProvider::_http_get(const std::string& url, std::string& response) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        LOG(ERROR) << "Failed to initialize CURL";
        return -1;
    }
    response.clear();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, oss_curl_write_cb);
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
    if (_http_get(url, response) != 0) return -1;
    role_name = response;
    // trim whitespace
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
    size_t nl = role_name.find('\n');
    if (nl != std::string::npos) {
        LOG(WARNING) << "Multiple ECS RAM roles; using first: " << role_name.substr(0, nl);
        role_name = role_name.substr(0, nl);
    }
    return 0;
}

int ECSMetadataCredentialsProvider::_fetch_credentials_outside_lock(
        std::unique_ptr<AlibabaCloud::OSS::Credentials>& out_credentials,
        std::chrono::system_clock::time_point& out_expiration) {
    std::string role_name;
    if (_get_role_name(role_name) != 0) return -1;

    std::string url =
            std::string("http://") + METADATA_SERVICE_HOST + METADATA_SERVICE_PATH + role_name;
    std::string response;
    if (_http_get(url, response) != 0) return -1;

    rapidjson::Document doc;
    doc.Parse(response.c_str());
    if (doc.HasParseError() || !doc.HasMember("Code") ||
        std::string(doc["Code"].GetString()) != "Success") {
        LOG(ERROR) << "ECS metadata parse error or non-Success code";
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
        LOG(ERROR) << "Failed to parse ECS expiration: " << expiry_str;
        return -1;
    }

    out_expiration = std::chrono::system_clock::from_time_t(timegm(&tm));
    out_credentials = std::make_unique<AlibabaCloud::OSS::Credentials>(ak, sk, token);
    VLOG(1) << "ECS credentials: ak=" << mask_credential(ak) << ", expiry=" << expiry_str;
    return 0;
}

// ── OSSSTSCredentialProvider ──────────────────────────────────────────────────

OSSSTSCredentialProvider::OSSSTSCredentialProvider(const std::string& role_arn,
                                                   const std::string& region,
                                                   const std::string& external_id,
                                                   const std::string& ca_cert_path,
                                                   const std::string& sts_endpoint)
        : _cached_credentials(nullptr),
          _expiration(std::chrono::system_clock::now()),
          _role_arn(role_arn),
          _region(region),
          _external_id(external_id),
          _ca_cert_path(ca_cert_path),
          _sts_endpoint(sts_endpoint) {
    if (_role_arn.empty()) {
        throw std::invalid_argument("RAM role ARN cannot be empty for STS AssumeRole");
    }
    LOG(INFO) << "OSSSTSCredentialProvider: role_arn=" << _role_arn << ", region=" << _region;
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
            return *_cached_credentials;
        }
        if (!_cached_credentials) {
            LOG(INFO) << "Fetching STS AssumeRole credentials (first time)";
        }
    }

    std::unique_ptr<AlibabaCloud::OSS::Credentials> new_creds;
    std::chrono::system_clock::time_point new_expiry;

    if (_fetch_credentials_from_sts(new_creds, new_expiry) != 0) {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials) {
            LOG(WARNING) << "Using expired STS credentials as fallback";
            return *_cached_credentials;
        }
        LOG(ERROR) << "Failed to fetch STS credentials with no cached fallback";
        return AlibabaCloud::OSS::Credentials("", "", "");
    }

    std::lock_guard<std::mutex> lock(_mtx);
    if (_cached_credentials && !_is_expired()) {
        return *_cached_credentials;
    }
    _cached_credentials = std::move(new_creds);
    _expiration = new_expiry;
    auto t = std::chrono::system_clock::to_time_t(_expiration);
    struct tm tm_buf;
    LOG(INFO) << "STS credentials refreshed, expiry: "
              << std::put_time(localtime_r(&t, &tm_buf), "%Y-%m-%d %H:%M:%S");
    return *_cached_credentials;
}

int OSSSTSCredentialProvider::_fetch_credentials_from_sts(
        std::unique_ptr<AlibabaCloud::OSS::Credentials>& out_credentials,
        std::chrono::system_clock::time_point& out_expiration) {
    try {
        AlibabaCloud::Credentials::Models::Config cred_config;
        cred_config.setType("ecs_ram_role");
        AlibabaCloud::Credentials::Client cred_client(cred_config);
        AlibabaCloud::Credentials::Models::CredentialModel base_cred = cred_client.getCredential();

        AlibabaCloud::OpenApi::Utils::Models::Config config;
        config.setAccessKeyId(base_cred.getAccessKeyId());
        config.setAccessKeySecret(base_cred.getAccessKeySecret());
        if (!base_cred.getSecurityToken().empty()) {
            config.setSecurityToken(base_cred.getSecurityToken());
        }
        config.setRegionId(_region);
        config.setEndpoint(_sts_endpoint.empty() ? "sts." + _region + ".aliyuncs.com"
                                                 : _sts_endpoint);

        AlibabaCloud::Sts20150401::Client client(config);

        AlibabaCloud::Sts20150401::Models::AssumeRoleRequest request;
        request.setRoleArn(_role_arn);
        char hostname_buf[64] = {};
        if (gethostname(hostname_buf, sizeof(hostname_buf) - 1) != 0) {
            strncpy(hostname_buf, "unknown", 7);
        }
        std::string session_name = "doris-";
        for (int i = 0; hostname_buf[i] != '\0' && session_name.size() < 32; ++i) {
            char c = hostname_buf[i];
            if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' || c == '.') {
                session_name += c;
            }
        }
        if (session_name.size() < 2) session_name = "doris-oss-session";
        request.setRoleSessionName(session_name);
        request.setDurationSeconds(SESSION_DURATION_SECONDS);
        if (!_external_id.empty()) request.setExternalId(_external_id);

        Darabonba::RuntimeOptions runtime;
        if (!_ca_cert_path.empty()) {
            runtime.setCa(_ca_cert_path);
        } else {
            runtime.setIgnoreSSL(true);
        }

        auto response = client.assumeRoleWithOptions(request, runtime);
        auto creds = response.getBody().getCredentials();

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

// ── OSSDefaultCredentialsProvider ─────────────────────────────────────────────

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
            return *_cached_credentials;
        }
    }

    try {
        AlibabaCloud::Credentials::Client cred_client;
        auto cred_model = cred_client.getCredential();
        std::string ak = cred_model.getAccessKeyId();
        std::string sk = cred_model.getAccessKeySecret();
        std::string token = cred_model.getSecurityToken();

        if (ak.empty() || sk.empty()) {
            std::lock_guard<std::mutex> lock(_mtx);
            if (_cached_credentials) {
                LOG(WARNING) << "Default provider returned empty credentials, using fallback";
                return *_cached_credentials;
            }
            LOG(ERROR) << "Default provider returned empty credentials with no fallback";
            return AlibabaCloud::OSS::Credentials("", "", "");
        }

        auto new_creds = token.empty()
                                 ? std::make_unique<AlibabaCloud::OSS::Credentials>(ak, sk)
                                 : std::make_unique<AlibabaCloud::OSS::Credentials>(ak, sk, token);
        auto new_expiry = std::chrono::system_clock::now() + std::chrono::hours(1);

        std::lock_guard<std::mutex> lock(_mtx);
        _cached_credentials = std::move(new_creds);
        _expiration = new_expiry;
        LOG(INFO) << "Default provider credentials fetched, provider: "
                  << cred_model.getProviderName();
        return *_cached_credentials;
    } catch (const std::exception& e) {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_cached_credentials) {
            LOG(WARNING) << "Default provider failed, using fallback: " << e.what();
            return *_cached_credentials;
        }
        LOG(ERROR) << "Default provider failed with no fallback: " << e.what();
        return AlibabaCloud::OSS::Credentials("", "", "");
    }
}

} // namespace doris

#endif // USE_OSS
