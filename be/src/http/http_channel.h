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

#include <stddef.h>

#include <string>

#include "http/http_status.h"

struct bufferevent_rate_limit_group;
namespace doris {

class HttpRequest;

class HttpChannel {
public:
    // Helper maybe used everywhere
    static void send_basic_challenge(HttpRequest* req, const std::string& realm);

    static void send_error(HttpRequest* request, HttpStatus status);

    // send 200(OK) reply with content
    static inline void send_reply(HttpRequest* request, const std::string& content) {
        send_reply(request, HttpStatus::OK, content);
    }

    static void send_reply(HttpRequest* request, HttpStatus status = HttpStatus::OK);

    static void send_reply(HttpRequest* request, HttpStatus status, const std::string& content);

    static void send_file(HttpRequest* request, int fd, size_t off, size_t size,
                          bufferevent_rate_limit_group* rate_limit_group = nullptr);

    static bool compress_content(const std::string& accept_encoding, const std::string& input,
                                 std::string* output);
};

} // namespace doris
