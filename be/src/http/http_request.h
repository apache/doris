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

#include <glog/logging.h>

#include <future>
#include <map>
#include <memory>
#include <string>

#include "common/config.h"
#include "http/http_method.h"
#include "util/string_util.h"

struct evhttp_request;

namespace doris {

class HttpHandler;

enum SendReplyType { REPLY_SYNC = 0, REPLY_ASYNC = 1 };

class HttpRequest {
public:
    HttpRequest(evhttp_request* ev_req);

    ~HttpRequest();

    int init_from_evhttp();

    HttpMethod method() const { return _method; }

    // path + '?' + query
    const std::string& uri() const { return _uri; }

    // return raw path without query string after '?'
    const std::string& raw_path() const { return _raw_path; }

    const std::string& header(const std::string& key) const;

    const std::string& param(const std::string& key) const;

    // return params
    const StringCaseUnorderedMap<std::string>& headers() { return _headers; }

    // return params
    std::map<std::string, std::string>* params() { return &_params; }

    const std::map<std::string, std::string>& query_params() const { return _query_params; }

    std::string get_request_body();

    void add_output_header(const char* key, const char* value);

    std::string debug_string() const;

    void set_handler(HttpHandler* handler) { _handler = handler; }
    HttpHandler* handler() const { return _handler; }

    struct evhttp_request* get_evhttp_request() const { return _ev_req; }

    std::shared_ptr<void> handler_ctx() const { return _handler_ctx; }
    void set_handler_ctx(std::shared_ptr<void> ctx) {
        DCHECK(_handler != nullptr);
        _handler_ctx = ctx;
    }

    const char* remote_host() const;

    void mark_send_reply(SendReplyType type = REPLY_ASYNC) { _send_reply_type = type; }

    void finish_send_reply();
    void wait_finish_send_reply();
private:
    SendReplyType _send_reply_type = REPLY_SYNC;
    HttpMethod _method;
    std::string _uri;
    std::string _raw_path;

    StringCaseUnorderedMap<std::string> _headers;
    std::map<std::string, std::string> _params;
    std::map<std::string, std::string> _query_params;

    struct evhttp_request* _ev_req = nullptr;
    HttpHandler* _handler = nullptr;

    std::shared_ptr<void> _handler_ctx;
    std::string _request_body;

    // ensure send_reply finished
    std::promise<bool> promise;
    std::future<bool> _futrue = promise.get_future();
};

} // namespace doris
