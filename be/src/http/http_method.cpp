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

#include "http/http_method.h"

#include <map>
#include <string>

namespace doris {

static std::map<std::string, HttpMethod> s_method_by_desc = {
        {"GET", HttpMethod::GET},       {"PUT", HttpMethod::PUT},
        {"POST", HttpMethod::POST},     {"HEAD", HttpMethod::HEAD},
        {"DELETE", HttpMethod::DELETE}, {"OPTIONS", HttpMethod::OPTIONS},
};
static std::map<HttpMethod, std::string> s_desc_by_method = {
        {HttpMethod::GET, "GET"},       {HttpMethod::PUT, "PUT"},
        {HttpMethod::POST, "POST"},     {HttpMethod::HEAD, "HEAD"},
        {HttpMethod::DELETE, "DELETE"}, {HttpMethod::OPTIONS, "OPTIONS"},
};

HttpMethod to_http_method(const char* desc) {
    auto iter = s_method_by_desc.find(desc);
    if (iter == s_method_by_desc.end()) {
        return HttpMethod::UNKNOWN;
    }
    return iter->second;
}

std::string to_method_desc(const HttpMethod& method) {
    auto iter = s_desc_by_method.find(method);
    if (iter == s_desc_by_method.end()) {
        return "UNKNOWN";
    }
    return iter->second;
}

} // namespace doris
