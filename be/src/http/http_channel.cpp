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
#include <event2/http_struct.h>

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
    auto* evb = evbuffer_new();
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
    auto* evb = evbuffer_new();
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

void HttpChannel::send_files(HttpRequest* request, const std::string& root_dir,
                             std::vector<std::string> local_files,
                             bufferevent_rate_limit_group* rate_limit_group) {
    if (rate_limit_group) {
        auto* evhttp_request = request->get_evhttp_request();
        auto* evhttp_connection = evhttp_request_get_connection(evhttp_request);
        auto* buffer_event = evhttp_connection_get_bufferevent(evhttp_connection);
        bufferevent_add_to_rate_limit_group(buffer_event, rate_limit_group);
    }

    send_files(request, root_dir, std::move(local_files));
}

void HttpChannel::send_files(HttpRequest* request, const std::string& root_dir,
                             std::vector<std::string> local_files) {
    std::unique_ptr<evbuffer, decltype(&evbuffer_free)> evb(evbuffer_new(), &evbuffer_free);
    for (const std::string& file : local_files) {
        std::string file_path = fmt::format("{}/{}", root_dir, file);
        int fd = open(file_path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::string error_msg = "Failed to open file: " + file_path;
            LOG(WARNING) << "http channel send files: " << error_msg;
            HttpChannel::send_reply(request, HttpStatus::NOT_FOUND, error_msg);
            return;
        }
        struct stat st;
        auto res = fstat(fd, &st);
        if (res < 0) {
            close(fd);
            std::string error_msg = "Failed to open file: " + file_path;
            LOG(WARNING) << "http channel send files: " << error_msg;
            HttpChannel::send_reply(request, HttpStatus::NOT_FOUND, error_msg);
            return;
        }

        int64_t file_size = st.st_size;
        VLOG_DEBUG << "http channel send file " << file_path << ", size: " << file_size;

        evbuffer_add_printf(evb.get(), "File-Name: %s\r\n", file.c_str());
        evbuffer_add_printf(evb.get(), "Content-Length: %ld\r\n", file_size);
        evbuffer_add_printf(evb.get(), "\r\n");
        if (file_size > 0) {
            evbuffer_add_file(evb.get(), fd, 0, file_size);
        }
    }

    evhttp_send_reply(request->get_evhttp_request(), HttpStatus::OK,
                      default_reason(HttpStatus::OK).c_str(), evb.get());
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
