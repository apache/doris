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

#include "http/http_client.h"

#include <glog/logging.h>
#include <unistd.h>

#include <memory>
#include <ostream>

#include "common/config.h"
#include "http/http_headers.h"
#include "http/http_status.h"
#include "util/stack_util.h"

namespace doris {

static const char* header_error_msg(CURLHcode code) {
    switch (code) {
    case CURLHE_OK:
        return "OK";
    case CURLHE_BADINDEX:
        return "header exists but not with this index ";
    case CURLHE_MISSING:
        return "no such header exists";
    case CURLHE_NOHEADERS:
        return "no headers at all exist (yet)";
    case CURLHE_NOREQUEST:
        return "no request with this number was used";
    case CURLHE_OUT_OF_MEMORY:
        return "out of memory while processing";
    case CURLHE_BAD_ARGUMENT:
        return "a function argument was not okay";
    case CURLHE_NOT_BUILT_IN:
        return "curl_easy_header() was disabled in the build";
    default:
        return "unknown";
    }
}

HttpClient::HttpClient() = default;

HttpClient::~HttpClient() {
    if (_curl != nullptr) {
        curl_easy_cleanup(_curl);
        _curl = nullptr;
    }
    if (_header_list != nullptr) {
        curl_slist_free_all(_header_list);
        _header_list = nullptr;
    }
}

Status HttpClient::init(const std::string& url, bool set_fail_on_error) {
    if (_curl == nullptr) {
        _curl = curl_easy_init();
        if (_curl == nullptr) {
            return Status::InternalError("fail to initialize curl");
        }
    } else {
        curl_easy_reset(_curl);
    }

    if (_header_list != nullptr) {
        curl_slist_free_all(_header_list);
        _header_list = nullptr;
    }
    // set error_buf
    _error_buf[0] = 0;
    auto code = curl_easy_setopt(_curl, CURLOPT_ERRORBUFFER, _error_buf);
    if (code != CURLE_OK) {
        LOG(WARNING) << "fail to set CURLOPT_ERRORBUFFER, msg=" << _to_errmsg(code);
        return Status::InternalError("fail to set error buffer");
    }
    // forbid signals
    code = curl_easy_setopt(_curl, CURLOPT_NOSIGNAL, 1L);
    if (code != CURLE_OK) {
        LOG(WARNING) << "fail to set CURLOPT_NOSIGNAL, msg=" << _to_errmsg(code);
        return Status::InternalError("fail to set CURLOPT_NOSIGNAL");
    }
    // set fail on error
    // When this option is set to `1L` (enabled), libcurl will return an error directly
    // when encountering HTTP error codes (>= 400), without reading the body of the error response.
    if (set_fail_on_error) {
        code = curl_easy_setopt(_curl, CURLOPT_FAILONERROR, 1L);
        if (code != CURLE_OK) {
            LOG(WARNING) << "fail to set CURLOPT_FAILONERROR, msg=" << _to_errmsg(code);
            return Status::InternalError("fail to set CURLOPT_FAILONERROR");
        }
    }
    // set redirect
    code = curl_easy_setopt(_curl, CURLOPT_FOLLOWLOCATION, 1L);
    if (code != CURLE_OK) {
        LOG(WARNING) << "fail to set CURLOPT_FOLLOWLOCATION, msg=" << _to_errmsg(code);
        return Status::InternalError("fail to set CURLOPT_FOLLOWLOCATION");
    }
    code = curl_easy_setopt(_curl, CURLOPT_MAXREDIRS, 20);
    if (code != CURLE_OK) {
        LOG(WARNING) << "fail to set CURLOPT_MAXREDIRS, msg=" << _to_errmsg(code);
        return Status::InternalError("fail to set CURLOPT_MAXREDIRS");
    }

    curl_write_callback callback = [](char* buffer, size_t size, size_t nmemb, void* param) {
        auto* client = (HttpClient*)param;
        return client->on_response_data(buffer, size * nmemb);
    };

    // set callback function
    code = curl_easy_setopt(_curl, CURLOPT_WRITEFUNCTION, callback);
    if (code != CURLE_OK) {
        LOG(WARNING) << "fail to set CURLOPT_WRITEFUNCTION, msg=" << _to_errmsg(code);
        return Status::InternalError("fail to set CURLOPT_WRITEFUNCTION");
    }
    code = curl_easy_setopt(_curl, CURLOPT_WRITEDATA, (void*)this);
    if (code != CURLE_OK) {
        LOG(WARNING) << "fail to set CURLOPT_WRITEDATA, msg=" << _to_errmsg(code);
        return Status::InternalError("fail to set CURLOPT_WRITEDATA");
    }

    std::string escaped_url;
    RETURN_IF_ERROR(_escape_url(url, &escaped_url));
    // set url
    code = curl_easy_setopt(_curl, CURLOPT_URL, escaped_url.c_str());
    if (code != CURLE_OK) {
        LOG(WARNING) << "failed to set CURLOPT_URL, errmsg=" << _to_errmsg(code);
        return Status::InternalError("fail to set CURLOPT_URL");
    }

    return Status::OK();
}

void HttpClient::set_method(HttpMethod method) {
    switch (method) {
    case GET:
        curl_easy_setopt(_curl, CURLOPT_HTTPGET, 1L);
        return;
    case PUT:
        curl_easy_setopt(_curl, CURLOPT_UPLOAD, 1L);
        return;
    case POST:
        curl_easy_setopt(_curl, CURLOPT_POST, 1L);
        return;
    case DELETE:
        curl_easy_setopt(_curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        return;
    case HEAD:
        curl_easy_setopt(_curl, CURLOPT_NOBODY, 1L);
        return;
    case OPTIONS:
        curl_easy_setopt(_curl, CURLOPT_CUSTOMREQUEST, "OPTIONS");
        return;
    default:
        return;
    }
}

size_t HttpClient::on_response_data(const void* data, size_t length) {
    if (*_callback != nullptr) {
        bool is_continue = (*_callback)(data, length);
        if (!is_continue) {
            return -1;
        }
    }
    return length;
}

// Status HttpClient::execute_post_request(const std::string& post_data, const std::function<bool(const void* data, size_t length)>& callback = {}) {
//     _callback = &callback;
//     set_post_body(post_data);
//     return execute(callback);
// }

Status HttpClient::execute_post_request(const std::string& payload, std::string* response) {
    set_method(POST);
    set_payload(payload);
    return execute(response);
}

Status HttpClient::execute_delete_request(const std::string& payload, std::string* response) {
    set_method(DELETE);
    set_payload(payload);
    return execute(response);
}

Status HttpClient::execute(const std::function<bool(const void* data, size_t length)>& callback) {
    _callback = &callback;
    auto code = curl_easy_perform(_curl);
    if (code != CURLE_OK) {
        LOG(WARNING) << "fail to execute HTTP client, errmsg=" << _to_errmsg(code)
                     << ", trace=" << get_stack_trace();
        return Status::HttpError(_to_errmsg(code));
    }
    return Status::OK();
}

Status HttpClient::get_content_md5(std::string* md5) const {
    struct curl_header* header_ptr;
    auto code = curl_easy_header(_curl, HttpHeaders::CONTENT_MD5, 0, CURLH_HEADER, 0, &header_ptr);
    if (code == CURLHE_MISSING || code == CURLHE_NOHEADERS) {
        // no such headers exists
        md5->clear();
        return Status::OK();
    } else if (code != CURLHE_OK) {
        auto msg = fmt::format("failed to get http header {}: {} ({})", HttpHeaders::CONTENT_MD5,
                               header_error_msg(code), code);
        LOG(WARNING) << msg << ", trace=" << get_stack_trace();
        return Status::HttpError(std::move(msg));
    }

    *md5 = header_ptr->value;
    return Status::OK();
}

Status HttpClient::download(const std::string& local_path) {
    // set method to GET
    set_method(GET);

    // TODO(zc) Move this download speed limit outside to limit download speed
    // at system level
    curl_easy_setopt(_curl, CURLOPT_LOW_SPEED_LIMIT, config::download_low_speed_limit_kbps * 1024);
    curl_easy_setopt(_curl, CURLOPT_LOW_SPEED_TIME, config::download_low_speed_time);
    curl_easy_setopt(_curl, CURLOPT_MAX_RECV_SPEED_LARGE, config::max_download_speed_kbps * 1024);

    auto fp_closer = [](FILE* fp) { fclose(fp); };
    std::unique_ptr<FILE, decltype(fp_closer)> fp(fopen(local_path.c_str(), "w"), fp_closer);
    if (fp == nullptr) {
        LOG(WARNING) << "open file failed, file=" << local_path;
        return Status::InternalError("open file failed");
    }
    Status status;
    auto callback = [&status, &fp, &local_path](const void* data, size_t length) {
        auto res = fwrite(data, length, 1, fp.get());
        if (res != 1) {
            LOG(WARNING) << "fail to write data to file, file=" << local_path
                         << ", error=" << ferror(fp.get());
            status = Status::InternalError("fail to write data when download");
            return false;
        }
        return true;
    };
    RETURN_IF_ERROR(execute(callback));
    return status;
}

Status HttpClient::execute(std::string* response) {
    auto callback = [response](const void* data, size_t length) {
        response->append((char*)data, length);
        return true;
    };
    return execute(callback);
}

const char* HttpClient::_to_errmsg(CURLcode code) {
    if (_error_buf[0] == 0) {
        return curl_easy_strerror(code);
    }
    return _error_buf;
}

Status HttpClient::execute_with_retry(int retry_times, int sleep_time,
                                      const std::function<Status(HttpClient*)>& callback) {
    Status status;
    for (int i = 0; i < retry_times; ++i) {
        HttpClient client;
        status = callback(&client);
        if (status.ok()) {
            auto http_status = client.get_http_status();
            if (http_status == 200) {
                return status;
            } else {
                auto error_msg = fmt::format("http status code is not 200, code={}", http_status);
                LOG(WARNING) << error_msg;
                return Status::HttpError(error_msg);
            }
        }
        sleep(sleep_time);
    }
    return status;
}

// http://example.com/page?param1=value1&param2=value+with+spaces#section
Status HttpClient::_escape_url(const std::string& url, std::string* escaped_url) {
    size_t query_pos = url.find('?');
    if (query_pos == std::string::npos) {
        *escaped_url = url;
        return Status::OK();
    }
    size_t fragment_pos = url.find('#');
    std::string query;
    std::string fragment;

    if (fragment_pos == std::string::npos) {
        query = url.substr(query_pos + 1, url.length() - query_pos - 1);
    } else {
        query = url.substr(query_pos + 1, fragment_pos - query_pos - 1);
        fragment = url.substr(fragment_pos, url.length() - fragment_pos);
    }

    std::string encoded_query;
    size_t ampersand_pos = query.find('&');
    size_t equal_pos;

    if (ampersand_pos == std::string::npos) {
        ampersand_pos = query.length();
    }

    while (true) {
        equal_pos = query.find('=');
        if (equal_pos != std::string::npos) {
            std::string key = query.substr(0, equal_pos);
            std::string value = query.substr(equal_pos + 1, ampersand_pos - equal_pos - 1);

            auto encoded_value = std::unique_ptr<char, decltype(&curl_free)>(
                    curl_easy_escape(_curl, value.c_str(), value.length()), &curl_free);
            if (encoded_value) {
                encoded_query += key + "=" + std::string(encoded_value.get());
            } else {
                return Status::InternalError("escape url failed, url={}", url);
            }
        } else {
            encoded_query += query.substr(0, ampersand_pos);
        }

        if (ampersand_pos == query.length() || ampersand_pos == std::string::npos) {
            break;
        }

        encoded_query += "&";
        query = query.substr(ampersand_pos + 1);
        ampersand_pos = query.find('&');
    }
    *escaped_url = url.substr(0, query_pos + 1) + encoded_query + fragment;
    return Status::OK();
}

} // namespace doris
