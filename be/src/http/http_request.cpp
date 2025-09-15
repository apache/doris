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

#include "http/http_request.h"

#include <event2/buffer.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/keyvalq_struct.h>

#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <cctype>

#include "glog/logging.h"
#include "http/http_handler.h"

namespace doris {

static std::string s_empty = "";

HttpRequest::HttpRequest(evhttp_request* evhttp_request) : _ev_req(evhttp_request) {}

HttpRequest::~HttpRequest() {
    if (_handler_ctx != nullptr) {
        DCHECK(_handler != nullptr);
        _handler->free_handler_ctx(_handler_ctx);
    }
}

int HttpRequest::init_from_evhttp() {
    _method = to_http_method(evhttp_request_get_command(_ev_req));
    if (_method == HttpMethod::UNKNOWN) {
        LOG(WARNING) << "unknown method of HTTP request, method="
                     << evhttp_request_get_command(_ev_req);
        return -1;
    }
    _uri = evhttp_request_get_uri(_ev_req);
    
    // Helper function to decode URL-encoded UTF-8 strings
    auto decode_utf8_header = [](const std::string& encoded) -> std::string {
        std::string decoded;
        decoded.reserve(encoded.length());
        
        for (size_t i = 0; i < encoded.length(); ++i) {
            if (encoded[i] == '%' && i + 2 < encoded.length()) {
                // Check if next two characters are hex digits
                char c1 = encoded[i + 1];
                char c2 = encoded[i + 2];
                if (std::isxdigit(c1) && std::isxdigit(c2)) {
                    // Convert hex to char
                    int val = (std::isdigit(c1) ? c1 - '0' : std::tolower(c1) - 'a' + 10) * 16 +
                              (std::isdigit(c2) ? c2 - '0' : std::tolower(c2) - 'a' + 10);
                    decoded += static_cast<char>(val);
                    i += 2; // Skip the two hex digits
                } else {
                    decoded += encoded[i];
                }
            } else {
                decoded += encoded[i];
            }
        }
        return decoded;
    };
    
    // conver header
    auto headers = evhttp_request_get_input_headers(_ev_req);
    for (auto header = headers->tqh_first; header != nullptr; header = header->next.tqe_next) {
        std::string decoded_value = decode_utf8_header(header->value);
        if (header->key == std::string("columns")) {
            LOG(INFO) << "HTTP header 'columns' - original: [" << header->value << "], decoded: [" << decoded_value << "]";
        }
        _headers.emplace(header->key, decoded_value);
    }
    // parse
    auto ev_uri = evhttp_request_get_evhttp_uri(_ev_req);
    _raw_path = evhttp_uri_get_path(ev_uri);
    auto query = evhttp_uri_get_query(ev_uri);
    if (query == nullptr || *query == '\0') {
        return 0;
    }
    struct evkeyvalq params;
    auto res = evhttp_parse_query_str(query, &params);
    if (res < 0) {
        LOG(WARNING) << "parse query str failed, query=" << query;
        return res;
    }
    for (auto param = params.tqh_first; param != nullptr; param = param->next.tqe_next) {
        _query_params.emplace(param->key, param->value);
    }
    _params.insert(_query_params.begin(), _query_params.end());
    evhttp_clear_headers(&params);
    return 0;
}

std::string HttpRequest::debug_string() const {
    std::stringstream ss;
    ss << "HttpRequest: \n"
       << "method:" << _method << "\n"
       << "uri:" << _uri << "\n"
       << "raw_path:" << _raw_path << "\n"
       << "headers: \n";
    for (auto& iter : _headers) {
        ss << "key=" << iter.first << ", value=" << iter.second << "\n";
    }
    ss << "params: \n";
    for (auto& iter : _params) {
        ss << "key=" << iter.first << ", value=" << iter.second << "\n";
    }

    return ss.str();
}

const std::string& HttpRequest::header(const std::string& key) const {
    auto iter = _headers.find(key);
    if (iter == _headers.end()) {
        return s_empty;
    }
    return iter->second;
}

const std::string& HttpRequest::param(const std::string& key) const {
    auto iter = _params.find(key);
    if (iter == _params.end()) {
        return s_empty;
    }
    return iter->second;
}

std::string HttpRequest::get_all_headers() const {
    std::stringstream headers;
    for (const auto& header : _headers) {
        headers << header.first << ":" << header.second + ", ";
    }
    return headers.str();
}

void HttpRequest::add_output_header(const char* key, const char* value) {
    evhttp_add_header(evhttp_request_get_output_headers(_ev_req), key, value);
}

std::string HttpRequest::get_request_body() {
    if (!_request_body.empty()) {
        return _request_body;
    }
    // read buf
    auto evbuf = evhttp_request_get_input_buffer(_ev_req);
    if (evbuf == nullptr) {
        return _request_body;
    }
    auto length = evbuffer_get_length(evbuf);
    _request_body.resize(length);
    evbuffer_remove(evbuf, (char*)_request_body.data(), length);
    return _request_body;
}

const char* HttpRequest::remote_host() const {
    return _ev_req->remote_host;
}

} // namespace doris
