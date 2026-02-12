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

#include <curl/curl.h>
#include <rapidjson/document.h>

#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "common/logging.h"

// Include OSS SDK headers in implementation only
#include <alibabacloud/oss/auth/Credentials.h>
#include <alibabacloud/oss/auth/CredentialsProvider.h>

namespace doris {

// CURL write callback - appends data to string
static size_t curl_write_callback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    size_t total_size = size * nmemb;
    userp->append(static_cast<char*>(contents), total_size);
    return total_size;
}

ECSMetadataCredentialsProvider::ECSMetadataCredentialsProvider()
        : _cached_credentials(nullptr), _expiration(std::chrono::system_clock::now()) {
    LOG(INFO) << "ECSMetadataCredentialsProvider initialized for Alibaba Cloud OSS";
}

bool ECSMetadataCredentialsProvider::_is_expired() const {
    auto now = std::chrono::system_clock::now();
    auto time_until_expiry =
            std::chrono::duration_cast<std::chrono::seconds>(_expiration - now).count();

    // Refresh if expired or will expire in 5 minutes
    return time_until_expiry <= REFRESH_BEFORE_EXPIRY_SECONDS;
}

AlibabaCloud::OSS::Credentials ECSMetadataCredentialsProvider::getCredentials() {
    std::lock_guard<std::mutex> lock(_mtx);

    // Return cached credentials if still valid
    if (_cached_credentials != nullptr && !_is_expired()) {
        VLOG(2) << "Returning cached OSS credentials from ECS metadata provider";
        return *_cached_credentials;
    }

    // Log credential refresh
    if (_cached_credentials != nullptr) {
        auto expiry_time = std::chrono::system_clock::to_time_t(_expiration);
        LOG(INFO) << "OSS credentials expired or expiring soon (expiration: "
                  << std::put_time(std::localtime(&expiry_time), "%Y-%m-%d %H:%M:%S")
                  << "), fetching new credentials from ECS metadata service";
    } else {
        LOG(INFO) << "Fetching OSS credentials from ECS metadata service (first time)";
    }

    // Fetch new credentials
    int ret = _fetch_credentials_from_metadata();
    if (ret != 0) {
        LOG(ERROR) << "Failed to fetch OSS credentials from ECS metadata service, error code: "
                   << ret;
        // If we have cached credentials, return them even if expired (better than failing)
        if (_cached_credentials != nullptr) {
            LOG(WARNING) << "Using expired credentials as fallback";
            return *_cached_credentials;
        }
        // No cached credentials, throw exception
        throw std::runtime_error(
                "Failed to fetch OSS credentials from ECS metadata service, error code: " +
                std::to_string(ret));
    }

    // Log success
    auto expiry_time = std::chrono::system_clock::to_time_t(_expiration);
    LOG(INFO) << "Successfully fetched OSS credentials from ECS metadata service, "
              << "expiration: " << std::put_time(std::localtime(&expiry_time), "%Y-%m-%d %H:%M:%S")
              << ", next refresh in approximately "
              << std::chrono::duration_cast<std::chrono::minutes>(_expiration -
                                                                   std::chrono::system_clock::now())
                             .count() -
                         5
              << " minutes";

    return *_cached_credentials;
}

int ECSMetadataCredentialsProvider::_http_get(const std::string& url, std::string& response) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        LOG(ERROR) << "Failed to initialize CURL for ECS metadata request";
        return -1;
    }

    response.clear();

    // Set CURL options
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, METADATA_SERVICE_TIMEOUT_MS);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L); // Thread-safe signal handling

    // Perform HTTP request
    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        LOG(ERROR) << "HTTP request to ECS metadata service failed: " << curl_easy_strerror(res);
        curl_easy_cleanup(curl);
        return -1;
    }

    // Check HTTP response code
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    if (http_code != 200) {
        LOG(ERROR) << "ECS metadata service returned HTTP " << http_code << ": " << response;
        return -1;
    }

    VLOG(2) << "ECS metadata service HTTP GET success: " << url;
    return 0;
}

int ECSMetadataCredentialsProvider::_get_role_name(std::string& role_name) {
    // Build URL: http://100.100.100.200/latest/meta-data/ram/security-credentials/
    std::string url = std::string("http://") + METADATA_SERVICE_HOST + METADATA_SERVICE_PATH;

    std::string response;
    if (_http_get(url, response) != 0) {
        return -1;
    }

    // Trim whitespace from response
    role_name = response;
    // Remove leading whitespace
    role_name.erase(role_name.begin(),
                    std::find_if(role_name.begin(), role_name.end(),
                                 [](unsigned char ch) { return !std::isspace(ch); }));
    // Remove trailing whitespace
    role_name.erase(std::find_if(role_name.rbegin(), role_name.rend(),
                                  [](unsigned char ch) { return !std::isspace(ch); })
                            .base(),
                    role_name.end());

    if (role_name.empty()) {
        LOG(ERROR) << "No RAM role attached to this ECS instance. "
                   << "Please attach a RAM role with OSS permissions to the ECS instance.";
        return -1;
    }

    LOG(INFO) << "ECS RAM role detected: " << role_name;
    return 0;
}

int ECSMetadataCredentialsProvider::_get_credentials_from_role(const std::string& role_name) {
    // Build URL: http://100.100.100.200/latest/meta-data/ram/security-credentials/{role-name}
    std::string url =
            std::string("http://") + METADATA_SERVICE_HOST + METADATA_SERVICE_PATH + role_name;

    std::string response;
    if (_http_get(url, response) != 0) {
        return -1;
    }

    // Parse JSON response
    rapidjson::Document doc;
    doc.Parse(response.c_str());

    if (doc.HasParseError()) {
        LOG(ERROR) << "Failed to parse JSON response from ECS metadata service";
        return -1;
    }

    // Check response code
    if (!doc.HasMember("Code") || std::string(doc["Code"].GetString()) != "Success") {
        std::string error_msg =
                doc.HasMember("Message") ? doc["Message"].GetString() : "Unknown error";
        LOG(ERROR) << "ECS metadata service returned error: " << error_msg;
        return -1;
    }

    // Extract required fields
    if (!doc.HasMember("AccessKeyId") || !doc.HasMember("AccessKeySecret") ||
        !doc.HasMember("SecurityToken") || !doc.HasMember("Expiration")) {
        LOG(ERROR) << "ECS metadata service response missing required fields. "
                   << "Expected: AccessKeyId, AccessKeySecret, SecurityToken, Expiration";
        return -1;
    }

    std::string access_key_id = doc["AccessKeyId"].GetString();
    std::string access_key_secret = doc["AccessKeySecret"].GetString();
    std::string security_token = doc["SecurityToken"].GetString();
    std::string expiration_str = doc["Expiration"].GetString();

    // Validate credentials are not empty
    if (access_key_id.empty() || access_key_secret.empty() || security_token.empty()) {
        LOG(ERROR) << "ECS metadata service returned empty credentials";
        return -1;
    }

    // Parse expiration time (ISO 8601 format: "2025-02-10T15:30:00Z")
    std::tm tm = {};
    std::istringstream ss(expiration_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");

    if (ss.fail()) {
        LOG(ERROR) << "Failed to parse expiration time from ECS metadata: " << expiration_str;
        return -1;
    }

    // Convert to system_clock::time_point
    _expiration = std::chrono::system_clock::from_time_t(std::mktime(&tm));

    // Create new Credentials object
    _cached_credentials = std::make_unique<AlibabaCloud::OSS::Credentials>(
            access_key_id, access_key_secret, security_token);

    VLOG(1) << "Parsed OSS credentials from ECS metadata: "
            << "AccessKeyId=" << access_key_id.substr(0, 8) << "..., "
            << "Expiration=" << expiration_str;

    return 0;
}

int ECSMetadataCredentialsProvider::_fetch_credentials_from_metadata() {
    // Step 1: Get RAM role name
    std::string role_name;
    if (_get_role_name(role_name) != 0) {
        return -1;
    }

    // Step 2: Get credentials for the role
    if (_get_credentials_from_role(role_name) != 0) {
        return -1;
    }

    return 0;
}

} // namespace doris
