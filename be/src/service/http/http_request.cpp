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

#include "service/http/http_request.h"

#include <event2/buffer.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/keyvalq_struct.h>

#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include "load/stream_load/stream_load_context.h"
#include "service/http/http_handler.h"
#include "service/http/http_headers.h"
#include "util/stack_util.h"
#include "util/url_coding.h"

namespace doris {

static std::string s_empty = "";

static const std::string kMasked = "***MASKED***";

// Helper function to check if a header should be masked in logs
static bool is_sensitive_header(const std::string& header_name) {
    return iequal(header_name, HttpHeaders::AUTHORIZATION) ||
           iequal(header_name, HttpHeaders::PROXY_AUTHORIZATION) || iequal(header_name, "token") ||
           iequal(header_name, HttpHeaders::AUTH_TOKEN) || iequal(header_name, "auth_code");
}

// A query parameter is sensitive when it carries one of the credentials we already mask in
// headers -- the cluster token travels as the "token" parameter of the download endpoints --
// or when it names a config declared sensitive: /api/update_config takes its argument as
// "<config name>=<new value>" in the query string, so masking only headers would still leave
// a password in the log line.
static bool is_sensitive_param(const std::string& param_name) {
    return is_sensitive_header(param_name) || config::is_sensitive_config(param_name);
}

// Renders a sensitive header for logging. For HTTP Basic credentials the user name is kept,
// so that logs still answer "who issued this request", and only the password is masked,
// yielding "<user>:***MASKED***". Any other sensitive header, and any credential we fail to
// parse, is masked as a whole. The result is a rendering, not the header value: the real one
// is base64 encoded.
static std::string mask_sensitive_header(const std::string& name, const std::string& value) {
    if (!iequal(name, HttpHeaders::AUTHORIZATION)) {
        return kMasked;
    }

    // Expected form: "Basic <base64(user:password)>"
    auto pos = value.find(' ');
    if (pos == std::string::npos || !iequal(value.substr(0, pos), "Basic")) {
        return kMasked;
    }
    std::string decoded;
    if (!base64_decode(value.substr(pos + 1), &decoded)) {
        return kMasked;
    }
    // Note that the password may contain a colon, so split on the first one only.
    auto colon = decoded.find(':');
    if (colon == std::string::npos) {
        return kMasked;
    }
    return decoded.substr(0, colon) + ":" + kMasked;
}

// Renders a request URI with the value of every sensitive query parameter replaced. The query
// string is masked in place rather than rebuilt from the parsed parameters, so that a request
// which never reached init_from_evhttp() -- a malformed query string is reported by logging the
// request -- is masked too, and so that what the log shows still looks like the URI received.
static std::string mask_sensitive_query_params(const std::string& uri) {
    auto query_pos = uri.find('?');
    if (query_pos == std::string::npos) {
        return uri;
    }

    std::string masked = uri.substr(0, query_pos + 1);
    for (size_t pos = query_pos + 1; pos < uri.size();) {
        size_t end = uri.find('&', pos);
        if (end == std::string::npos) {
            end = uri.size();
        }
        std::string pair = uri.substr(pos, end - pos);
        size_t eq = pair.find('=');
        // A parameter with no '=' carries no value to leak.
        if (eq != std::string::npos) {
            const std::string raw_name = pair.substr(0, eq);
            std::string name;
            if (!url_decode(raw_name, &name)) {
                name = raw_name;
            }
            if (is_sensitive_param(name)) {
                pair = raw_name + "=" + kMasked;
            }
        }
        masked += pair;
        if (end < uri.size()) {
            masked += '&';
        }
        pos = end + 1;
    }
    return masked;
}

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
       << "uri:" << mask_sensitive_query_params(_uri) << "\n"
       << "raw_path:" << _raw_path << "\n"
       << "headers: \n";
    for (auto& iter : _headers) {
        if (is_sensitive_header(iter.first)) {
            ss << "key=" << iter.first
               << ", value=" << mask_sensitive_header(iter.first, iter.second) << "\n";
        } else {
            ss << "key=" << iter.first << ", value=" << iter.second << "\n";
        }
    }
    ss << "params: \n";
    for (auto& iter : _params) {
        if (is_sensitive_param(iter.first)) {
            ss << "key=" << iter.first << ", value=" << kMasked << "\n";
        } else {
            ss << "key=" << iter.first << ", value=" << iter.second << "\n";
        }
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
        // Mask sensitive headers
        if (is_sensitive_header(header.first)) {
            headers << header.first << ":" << kMasked << ", ";
        } else {
            headers << header.first << ":" << header.second + ", ";
        }
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

void HttpRequest::finish_send_reply() {
    if (_send_reply_type == REPLY_SYNC) {
        return;
    }

    std::string infos;
    if (_handler_ctx != nullptr) {
        infos = reinterpret_cast<StreamLoadContext*>(_handler_ctx.get())->brief();
    }
    _http_reply_promise.set_value(true);
}

void HttpRequest::wait_finish_send_reply() {
    if (_send_reply_type == REPLY_SYNC) {
        return;
    }

    std::string infos;
    StreamLoadContext* ctx = nullptr;
    if (_handler_ctx != nullptr) {
        ctx = reinterpret_cast<StreamLoadContext*>(_handler_ctx.get());
        infos = ctx->brief();
        _handler->free_handler_ctx(_handler_ctx);
    }

    VLOG_NOTICE << "start to wait send reply, infos=" << infos;
    auto status = _http_reply_future.wait_for(std::chrono::seconds(config::async_reply_timeout_s));
    // if request is timeout and can't cancel fragment in time, it will cause some new request block
    // so we will free cancelled request in time.
    if (status != std::future_status::ready) {
        LOG(WARNING) << "wait for send reply timeout, " << this->debug_string();
        std::unique_lock<std::mutex> lock1(ctx->_send_reply_lock);
        // do not send_reply after free current request
        ctx->_can_send_reply = false;
        ctx->_finish_send_reply = true;
        ctx->_can_send_reply_cv.notify_all();
    } else {
        VLOG_NOTICE << "wait send reply finished";
    }

    // delete _handler_ctx at the end, in case that finish_send_reply can't get detailed info
    _handler_ctx = nullptr;
}

} // namespace doris
