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
    // conver header
    auto headers = evhttp_request_get_input_headers(_ev_req);
    for (auto header = headers->tqh_first; header != nullptr; header = header->next.tqe_next) {
        _headers.emplace(header->key, header->value);
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
