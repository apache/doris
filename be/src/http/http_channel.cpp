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

#include "http/http_channel.h"

#include <sstream>
#include <string>

#include <event2/buffer.h>
#include <event2/http.h>

#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_headers.h"
#include "http/http_status.h"
#include "common/logging.h"

namespace doris {

// Send Unauthorized status with basic challenge
void HttpChannel::send_basic_challenge(HttpRequest* req, const std::string& realm) {
    static std::string s_prompt_str = "Please provide your userid and password\n";
    std::stringstream ss;
    ss << "Basic realm=\"" << realm << "\"";
    req->add_output_header(HttpHeaders::WWW_AUTHENTICATE, ss.str().c_str());
    send_reply(req, HttpStatus::UNAUTHORIZED, s_prompt_str);
}

void HttpChannel::send_error(HttpRequest* request, HttpStatus status) {
    evhttp_send_error(request->get_evhttp_request(), status, defalut_reason(status).c_str());
}

void HttpChannel::send_reply(HttpRequest* request, HttpStatus status) {
    evhttp_send_reply(request->get_evhttp_request(), status,
                      defalut_reason(status).c_str(), nullptr);
}

void HttpChannel::send_reply(
        HttpRequest* request, HttpStatus status, const std::string& content) {
    auto evb = evbuffer_new();
    evbuffer_add(evb, content.c_str(), content.size());
    evhttp_send_reply(request->get_evhttp_request(), status, defalut_reason(status).c_str(), evb);
    evbuffer_free(evb);
}

void HttpChannel::send_file(HttpRequest* request, int fd, size_t off, size_t size) {
    auto evb = evbuffer_new();
    evbuffer_add_file(evb, fd, off, size);
    evhttp_send_reply(request->get_evhttp_request(),
                      HttpStatus::OK,
                      defalut_reason(HttpStatus::OK).c_str(), evb);
    evbuffer_free(evb);
}

}
