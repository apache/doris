// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_COMMON_UTIL_HTTP_CHANNEL_H
#define BDG_PALO_BE_SRC_COMMON_UTIL_HTTP_CHANNEL_H

#include <string>
#include <cstdint>

struct mg_connection;

namespace palo {

class HttpRequest;
class HttpResponse;

class HttpChannel {
public:
    // Wrapper for mongoose
    HttpChannel(const HttpRequest& request, mg_connection* mg_conn);

    void send_response(const HttpResponse& response);

    void send_response_header(const HttpResponse& response);
    void send_response_content(const HttpResponse& response);
    void append_response_content(
            const HttpResponse& response,
            const char* content,
            int32_t content_size);
    void send_status_line();

    const HttpRequest& request() const {
        return _request;
    }

    void update_content_length(int64_t len);

    int read(char* buf, int len);

    // Helper maybe used everywhere
    void send_basic_challenge(const std::string& realm);

private:
    const HttpRequest& _request;
    // save mongoose connection here
    mg_connection* _mg_conn;
};

}

#endif
