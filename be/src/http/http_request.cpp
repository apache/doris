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

#include "http/http_request.h"

#include <string>
#include <sstream>
#include <vector>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>

#include "http/mongoose.h"

#include "common/logging.h"
#include "util/url_coding.h"

namespace palo {

static std::string s_empty = "";

HttpRequest::HttpRequest(mg_connection* conn) :
        _conn(conn) {
    init();
}

bool HttpRequest::init() {
    const mg_request_info* mg_req = mg_get_request_info(_conn);
    _method = to_http_method(mg_req->request_method);

    // Method
    if (_method == HttpMethod::UNKNOWN) {
        LOG(WARNING) << "Unknown method of http requeset " << mg_req;
        return false;
    }
    _uri = mg_req->uri;
    _raw_path = mg_req->uri;
    
    // convert header
    for (int i = 0; i < mg_req->num_headers; ++i) {
        _headers.insert(std::make_pair(mg_req->http_headers[i].name, 
                                       mg_req->http_headers[i].value));
    }

    // parse parameters in query string
    if (mg_req->query_string != nullptr) {
        parse_params(mg_req->query_string);
    }

    return true;
}

void HttpRequest::parse_params(const char* query) {
    std::vector<std::string> arg_pairs;
    // TODO(zc): remove boost
    boost::split(arg_pairs, query, boost::is_any_of("&"));

    BOOST_FOREACH(const std::string & arg_pair, arg_pairs) {
        std::vector<std::string> key_value;
        boost::split(key_value, arg_pair, boost::is_any_of("="));

        if (key_value.empty()) {
            continue;
        }

        std::string key;

        if (!url_decode(key_value[0], &key)) {
            continue;
        }

        std::string value;

        if (!url_decode((key_value.size() >= 2 ? key_value[1] : ""), &value)) {
            continue;
        }

        boost::algorithm::to_lower(key);
        _query_params[key] = value;
    }
    _params.insert(_query_params.begin(), _query_params.end());
}

std::string HttpRequest::debug_string() const {
    std::stringstream ss;
    ss << "HttpRequest: \n" 
        << "method:" << _method << "\n"
        << "uri:" << _uri << "\n"
        << "raw_path:" << _raw_path << "\n"
        << "headers: \n";
    for (auto& iter : _headers) {
        ss << "key=" << iter.first << ", value=" << iter.second << "\n";
    }
    ss << "params: \n";
    for (auto& iter : _params) {
        ss << "key=" << iter.first << ", value=" << iter.second << "\n";
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

}
