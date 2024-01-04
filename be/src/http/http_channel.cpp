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

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/http.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "util/slice.h"
#include "util/zlib.h"

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
    evhttp_send_error(request->get_evhttp_request(), status, default_reason(status).c_str());
}

void HttpChannel::send_reply(HttpRequest* request, HttpStatus status) {
    evhttp_send_reply(request->get_evhttp_request(), status, default_reason(status).c_str(),
                      nullptr);
}

void HttpChannel::send_reply(HttpRequest* request, HttpStatus status, const std::string& content) {
    auto evb = evbuffer_new();
    std::string compressed_content;
    if (compress_content(request->header(HttpHeaders::ACCEPT_ENCODING), content,
                         &compressed_content)) {
        request->add_output_header(HttpHeaders::CONTENT_ENCODING, "gzip");
        evbuffer_add(evb, compressed_content.c_str(), compressed_content.size());
    } else {
        evbuffer_add(evb, content.c_str(), content.size());
    }
    evhttp_send_reply(request->get_evhttp_request(), status, default_reason(status).c_str(), evb);
    evbuffer_free(evb);
}

void HttpChannel::send_file(HttpRequest* request, int fd, size_t off, size_t size,
                            bufferevent_rate_limit_group* rate_limit_group) {
    auto evb = evbuffer_new();
    evbuffer_add_file(evb, fd, off, size);
    auto* evhttp_request = request->get_evhttp_request();
    if (rate_limit_group) {
        auto* evhttp_connection = evhttp_request_get_connection(evhttp_request);
        auto* buffer_event = evhttp_connection_get_bufferevent(evhttp_connection);
        bufferevent_add_to_rate_limit_group(buffer_event, rate_limit_group);
    }
    evhttp_send_reply(evhttp_request, HttpStatus::OK, default_reason(HttpStatus::OK).c_str(), evb);
    evbuffer_free(evb);
}

bool HttpChannel::compress_content(const std::string& accept_encoding, const std::string& input,
                                   std::string* output) {
    // Don't bother compressing empty content.
    if (input.empty()) {
        return false;
    }

    // Check if gzip compression is accepted by the caller. If so, compress the
    // content and replace the prerendered output.
    bool is_compressed = false;
    std::vector<string> encodings = strings::Split(accept_encoding, ",");
    for (string& encoding : encodings) {
        StripWhiteSpace(&encoding);
        if (encoding == "gzip") {
            std::ostringstream oss;
            Status s = zlib::CompressLevel(Slice(input), 1, &oss);
            if (s.ok()) {
                *output = oss.str();
                is_compressed = true;
            } else {
                LOG(WARNING) << "Could not compress output: " << s.to_string();
            }
            break;
        }
    }
    return is_compressed;
}

} // namespace doris
