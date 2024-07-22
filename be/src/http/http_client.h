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

#include <curl/curl.h>
#include <curl/system.h>
#include <stdint.h>

#include <cstdio>
#include <functional>
#include <string>

#include "common/status.h"
#include "http/http_method.h"

namespace doris {

// Helper class to access HTTP resource
class HttpClient {
public:
    HttpClient();
    ~HttpClient();

    // you can call this function to execute HTTP request with retry,
    // if callback return OK, this function will end and return OK.
    // This function will return FAIL if three are more than retry_times
    // that callback return FAIL.
    static Status execute_with_retry(int retry_times, int sleep_time,
                                     const std::function<Status(HttpClient*)>& callback);

    // this function must call before other function,
    // you can call this multiple times to reuse this object
    Status init(const std::string& url, bool set_fail_on_error = true);

    void set_method(HttpMethod method);

    void set_basic_auth(const std::string& user, const std::string& passwd) {
        curl_easy_setopt(_curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
        curl_easy_setopt(_curl, CURLOPT_USERNAME, user.c_str());
        curl_easy_setopt(_curl, CURLOPT_PASSWORD, passwd.c_str());
    }

    // content_type such as "application/json"
    void set_content_type(const std::string content_type) {
        std::string scratch_str = "Content-Type: " + content_type;
        _header_list = curl_slist_append(_header_list, scratch_str.c_str());
        curl_easy_setopt(_curl, CURLOPT_HTTPHEADER, _header_list);
    }

    void set_payload(const std::string& post_body) {
        curl_easy_setopt(_curl, CURLOPT_POSTFIELDSIZE, (long)post_body.length());
        curl_easy_setopt(_curl, CURLOPT_COPYPOSTFIELDS, post_body.c_str());
    }

    // Currently, only fake SSL configurations are supported
    void use_untrusted_ssl() {
        curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(_curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }

    // TODO(zc): support set header
    // void set_header(const std::string& key, const std::string& value) {
    // _cntl.http_request().SetHeader(key, value);
    // }

    std::string get_response_content_type() {
        char* ct = nullptr;
        auto code = curl_easy_getinfo(_curl, CURLINFO_CONTENT_TYPE, &ct);
        if (code == CURLE_OK && ct != nullptr) {
            return ct;
        }
        return std::string();
    }

    // Set the long gohead parameter to 1L to continue send authentication (user+password)
    // credentials when following locations, even when hostname changed.
    void set_unrestricted_auth(int gohead) {
        curl_easy_setopt(_curl, CURLOPT_UNRESTRICTED_AUTH, gohead);
    }

    void set_timeout_ms(int64_t timeout_ms) {
        curl_easy_setopt(_curl, CURLOPT_TIMEOUT_MS, timeout_ms);
    }

    // used to get content length
    // return -1 as error
    Status get_content_length(uint64_t* length) const {
        curl_off_t cl;
        auto code = curl_easy_getinfo(_curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &cl);
        if (!code) {
            if (cl < 0) {
                return Status::InternalError(
                        fmt::format("failed to get content length, it should be a positive value, "
                                    "actual is : {}",
                                    cl));
            }
            *length = (uint64_t)cl;
            return Status::OK();
        }
        return Status::InternalError("failed to get content length. err code: {}", code);
    }

    // Get the value of the header CONTENT-MD5. The output is empty if no such header exists.
    Status get_content_md5(std::string* md5) const;

    long get_http_status() const {
        long code;
        curl_easy_getinfo(_curl, CURLINFO_RESPONSE_CODE, &code);
        return code;
    }

    // execute a head method
    Status head() {
        set_method(HEAD);
        return execute();
    }

    // helper function to download a file, you can call this function to download
    // a file to local_path
    Status download(const std::string& local_path);

    Status execute_post_request(const std::string& payload, std::string* response);

    Status execute_delete_request(const std::string& payload, std::string* response);

    // execute a simple method, and its response is saved in response argument
    Status execute(std::string* response);

    // execute remote call action
    Status execute(const std::function<bool(const void* data, size_t length)>& callback = {});

    size_t on_response_data(const void* data, size_t length);

private:
    const char* _to_errmsg(CURLcode code);

private:
    CURL* _curl = nullptr;
    using HttpCallback = std::function<bool(const void* data, size_t length)>;
    const HttpCallback* _callback = nullptr;
    char _error_buf[CURL_ERROR_SIZE];
    curl_slist* _header_list = nullptr;
};

} // namespace doris
