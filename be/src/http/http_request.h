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

#ifndef BDG_PALO_BE_SRC_COMMON_UTIL_HTTP_REQUEST_H
#define BDG_PALO_BE_SRC_COMMON_UTIL_HTTP_REQUEST_H

#include <map>
#include <string>

#include "http/http_method.h"

struct mg_connection;

namespace palo {

class HttpRequest {
public:
    // Now we only construct http request from mongoose
    HttpRequest(mg_connection* conn);

    ~HttpRequest() {
    }

    HttpMethod method() const {
        return _method;
    }

    // path + '?' + query
    const std::string& uri() const {
        return _uri;
    }

    // return raw path without query string after '?'
    const std::string& raw_path() const {
        return _raw_path;
    }

    const std::string& header(const std::string& key) const;

    const std::string& param(const std::string& key) const;

    // return params
    const std::map<std::string, std::string>& headers() {
        return _headers;
    }

    // return params
    std::map<std::string, std::string>* params() {
        return &_params;
    }

    const std::map<std::string, std::string>& query_params() const {
        return _query_params;
    }

    std::string debug_string() const;

private:
    // construct from mg_connection
    bool init();

    void parse_params(const char* query);

    HttpMethod _method;
    std::string _uri;
    std::string _raw_path;
    std::map<std::string, std::string> _headers;
    std::map<std::string, std::string> _params;
    std::map<std::string, std::string> _query_params;

    // save mongoose connection here
    mg_connection* _conn;
};

}

#endif
