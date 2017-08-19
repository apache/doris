// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_headers.h"
#include "http/http_status.h"
#include "http/mongoose.h"
#include "common/logging.h"

namespace palo {

HttpChannel::HttpChannel(const HttpRequest& request, mg_connection* mg_conn) :
        _request(request),
        _mg_conn(mg_conn) {
}

void HttpChannel::send_response(const HttpResponse& response) {
    // Send status line
    mg_printf(_mg_conn, "HTTP/1.1 %s %s\r\n",
              to_code(response.status()).c_str(),
              defalut_reason(response.status()).c_str());
    // send all headers
    for (auto& it : response.headers()) {
        mg_printf(_mg_conn, "%s: ", it.first.c_str());
        for (int i = 0; i < it.second.size(); ++i) {
            if (i != 0) {
                mg_printf(_mg_conn, ",");
            }
            mg_printf(_mg_conn, "%s", it.second[i].c_str());
        }
        mg_printf(_mg_conn, "\r\n");
    }
    // Content type
    const std::string* content = response.content();
    bool contain_content =  content != nullptr && !content->empty();
    if (contain_content) {
        mg_printf(_mg_conn, "%s: %s\r\n",
                  HttpHeaders::CONTENT_TYPE,
                  response.content_type().c_str());
        mg_printf(_mg_conn, "%s: %zu\r\n",
                  HttpHeaders::CONTENT_LENGTH, content->length());
    }
    // customer headers
    // End of header
    mg_printf(_mg_conn, "\r\n");
    if (contain_content) {
        mg_write(_mg_conn, content->c_str(), content->length());
    }
}

// Send Unauthorized status with basic challenge
void HttpChannel::send_basic_challenge(const std::string& realm) {
    static std::string s_prompt_str = "Please provide your userid and password\n";
    HttpResponse response(HttpStatus::UNAUTHORIZED, &s_prompt_str);
    std::stringstream ss;
    ss << "Basic realm=\"" << realm << "\"";
    response.add_header(HttpHeaders::WWW_AUTHENTICATE, ss.str());

    send_response(response);
}

void HttpChannel::send_response_header(const HttpResponse& response) {
    // Send status line
    mg_printf(_mg_conn, "HTTP/1.1 %s %s\r\n",
            to_code(response.status()).c_str(),
            defalut_reason(response.status()).c_str());

    // Headers
    auto headers = response.headers();
    auto iter = headers.begin();
    for (; iter != headers.end(); ++iter) {
        auto& header_values = iter->second;
        auto value_iter = header_values.begin();
        for (; value_iter != header_values.end(); ++value_iter) {
            mg_printf(_mg_conn, "%s: %s\r\n",
                    iter->first.c_str(),
                    value_iter->c_str());
        }
    }

    // End of header
    mg_printf(_mg_conn, "\r\n");
}

void HttpChannel::send_response_content(const HttpResponse& response) {
    const std::string* content = response.content();
    bool contain_content =  content != nullptr && !content->empty();
    if (contain_content) {
        mg_write(_mg_conn, content->c_str(), content->length());
    }
}

void HttpChannel::append_response_content(
        const HttpResponse& response,
        const char* content,
        int32_t content_size) {
    mg_write(_mg_conn, content, content_size);
}

int HttpChannel::read(char* buf, int len) {
    return mg_read(_mg_conn, buf, len);
}
void HttpChannel::update_content_length(int64_t len) {
}

}
