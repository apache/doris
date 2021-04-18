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

#include "http/http_response.h"

namespace doris {

static std::string s_text_content_type = "text/plain; charset=UTF-8";

HttpResponse::HttpResponse(const HttpStatus& status)
        : _status(status), _content_type(s_text_content_type), _content(nullptr) {}

HttpResponse::HttpResponse(const HttpStatus& status, const std::string* content)
        : _status(status), _content_type(s_text_content_type), _content(content) {}

HttpResponse::HttpResponse(const HttpStatus& status, const std::string& type,
                           const std::string* content)
        : _status(status), _content_type(type), _content(content) {}

void HttpResponse::add_header(const std::string& key, const std::string& value) {
    _custom_headers[key].push_back(value);
}

} // namespace doris
