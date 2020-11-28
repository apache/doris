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

#include "http/http_status.h"

#include <map>
#include <string>

namespace doris {

static std::map<HttpStatus, std::string> s_reason_map = {
        {HttpStatus::CONTINUE, "Continue"},
        {HttpStatus::SWITCHING_PROTOCOLS, "Switching Protocols"},
        {HttpStatus::OK, "OK"},
        {HttpStatus::CREATED, "Created"},
        {HttpStatus::ACCEPTED, "Accepted"},
        {HttpStatus::NON_AUTHORITATIVE_INFORMATION, "Non-Authoritative Information"},
        {HttpStatus::NO_CONTENT, "No Content"},
        {HttpStatus::RESET_CONTENT, "Reset Content"},
        {HttpStatus::PARTIAL_CONTENT, "Partial Content"},
        {HttpStatus::MULTIPLE_CHOICES, "Multiple Choices"},
        {HttpStatus::MOVED_PERMANENTLY, "Moved Permanently"},
        {HttpStatus::FOUND, "Found"},
        {HttpStatus::SEE_OTHER, "See Other"},
        {HttpStatus::NOT_MODIFIED, "Not Modified"},
        {HttpStatus::USE_PROXY, "Use Proxy"},
        {HttpStatus::TEMPORARY_REDIRECT, "Temporary Redirect"},
        {HttpStatus::BAD_REQUEST, "Bad Request"},
        {HttpStatus::UNAUTHORIZED, "Unauthorized"},
        {HttpStatus::PAYMENT_REQUIRED, "Payment Required"},
        {HttpStatus::FORBIDDEN, "Forbidden"},
        {HttpStatus::NOT_FOUND, "Not Found"},
        {HttpStatus::METHOD_NOT_ALLOWED, "Method Not Allowed"},
        {HttpStatus::NOT_ACCEPTABLE, "Not Acceptable"},
        {HttpStatus::PROXY_AUTHENTICATION, "Proxy Authentication Required"},
        {HttpStatus::REQUEST_TIMEOUT, "Request Time-out"},
        {HttpStatus::CONFLICT, "Conflict"},
        {HttpStatus::GONE, "Gone"},
        {HttpStatus::LENGTH_REQUIRED, "Length Required"},
        {HttpStatus::PRECONDITION_FAILED, "Precondition Failed"},
        {HttpStatus::REQUEST_ENTITY_TOO_LARGE, "Request Entity Too Large"},
        {HttpStatus::REQUEST_URI_TOO_LONG, "Request-URI Too Large"},
        {HttpStatus::UNSUPPORTED_MEDIA_TYPE, "Unsupported Media Type"},
        {HttpStatus::REQUESTED_RANGE_NOT_SATISFIED, "Requested range not satisfiable"},
        {HttpStatus::EXPECTATION_FAILED, "Expectation Failed"},
        {HttpStatus::INTERNAL_SERVER_ERROR, "Internal Server Error"},
        {HttpStatus::NOT_IMPLEMENTED, "Not Implemented"},
        {HttpStatus::BAD_GATEWAY, "Bad Gateway"},
        {HttpStatus::SERVICE_UNAVAILABLE, "Service Unavailable"},
        {HttpStatus::GATEWAY_TIMEOUT, "Gateway Time-out"},
        {HttpStatus::HTTP_VERSION_NOT_SUPPORTED, "HTTP Version not supported"}};

std::string default_reason(const HttpStatus& status) {
    auto iter = s_reason_map.find(status);
    if (iter != s_reason_map.end()) {
        return iter->second;
    }
    return "No reason";
}

std::string to_code(const HttpStatus& status) {
    return std::to_string(status);
}

} // namespace doris
